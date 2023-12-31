use crate::cmds::{parse_cmds, BCommand, CCode, Cmds, Delete, Next, Stats, Summary};
use crate::cmds::{Files, Find};
use crate::error::AppError;
use crate::log::dump_diagnostics;
use crate::proc3::threads::{init_work, Msg, Work};
#[allow(unused_imports)]
use crate::proc3::{
    auto_save, find_matched_lines, indexing, load_file, shut_down, Data, FileFilter,
};
use blockfile2::LogicalNr;
use kparse::prelude::*;
use kparse::Track;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;
#[cfg(feature = "allocator")]
use std::alloc::System;
use std::io::Write;
use std::path::PathBuf;
use std::process::exit;
#[cfg(feature = "allocator")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "allocator")]
use tracking_allocator::{AllocationGroupId, AllocationRegistry, AllocationTracker, Allocator};

mod cmdlib;
mod cmds;
mod error;
pub mod index2;
mod log;
pub mod proc3;

#[cfg(feature = "allocator")]
#[global_allocator]
static GLOBAL: Allocator<System> = Allocator::system();

#[cfg(feature = "allocator")]
struct StdoutTracker {
    n: AtomicUsize,
    accu: [AtomicUsize; 20],
}

// This is our tracker implementation.  You will always need to create an implementation of `AllocationTracker` in order
// to actually handle allocation events.  The interface is straightforward: you're notified when an allocation occurs,
// and when a deallocation occurs.
#[cfg(feature = "allocator")]
impl AllocationTracker for StdoutTracker {
    fn allocated(
        &self,
        _addr: usize,
        _object_size: usize,
        wrapped_size: usize,
        group_id: AllocationGroupId,
    ) {
        let n = self.n.fetch_add(1, Ordering::Acquire);
        self.accu[group_id.as_usize().get()].fetch_add(wrapped_size, Ordering::Acquire);

        AllocationRegistry::untracked(|| {
            if n % 1000000 == 0 {
                for i in 0..self.accu.len() {
                    let v = self.accu[i].load(Ordering::Relaxed);
                    if v > 0 {
                        print!(" {}={}MB", i, v / 1_000_000);
                    }
                }
                println!();
            }
        });
    }

    fn deallocated(
        &self,
        _addr: usize,
        _object_size: usize,
        wrapped_size: usize,
        source_group_id: AllocationGroupId,
        _current_group_id: AllocationGroupId,
    ) {
        self.accu[source_group_id.as_usize().get()].fetch_sub(wrapped_size, Ordering::Acquire);
    }
}

fn main() -> Result<(), AppError> {
    #[cfg(feature = "allocator")]
    let trk = StdoutTracker {
        n: AtomicUsize::new(0),
        accu: [
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ],
    };
    #[cfg(feature = "allocator")]
    let _ = AllocationRegistry::set_global_tracker(trk).expect("global-tracker");

    println!("loading");
    let stored = PathBuf::from("stored.idx");
    let data = match Data::read(&stored) {
        Ok(v) => v,
        Err(e) => {
            println!("{:?}", e);
            exit(1234);
        }
    };

    let mut rl = Editor::<Cmds, FileHistory>::new()?;
    rl.set_helper(Some(Cmds));
    let _ = rl.load_history("history.txt");

    println!("spinup");
    let work: &'static Work = Box::leak(Box::new(init_work(rl.create_external_printer()?, data)));

    println!("enable_tracking");
    #[cfg(feature = "allocator")]
    AllocationRegistry::enable_tracking();

    let mut break_flag = false;
    loop {
        match rl.readline("> ") {
            Ok(txt_input) if txt_input.len() > 0 => {
                break_flag = false;
                rl.add_history_entry(txt_input.as_str())?;
                match parse_cmd(data, work, &txt_input, &mut rl) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("parse_cmd {:#?}", e);
                    }
                }
            }
            Ok(_) => {}
            Err(ReadlineError::Interrupted) => {
                eprintln!("CTRL-C");
                if break_flag {
                    break;
                } else {
                    break_flag = true;
                }
            }
            Err(ReadlineError::Eof) => {
                eprintln!("CTRL-D");
                break;
            }
            Err(err) => {
                eprintln!("readline {:?}", err);
            }
        }
    }

    shut_down(work);
    auto_save(&work.printer.clone(), data)?;

    rl.save_history("history.txt")?;

    Ok(())
}

fn parse_cmd(
    data: &'static Data,
    work: &'static Work,
    txt: &str,
    _rl: &mut Editor<Cmds, FileHistory>,
) -> Result<(), AppError> {
    let trk = Track::new_tracker::<CCode, _>();
    let span = Track::new_span(&trk, txt);

    let bcmd = match parse_cmds(span) {
        Ok((_, bcmd)) => bcmd,
        Err(nom::Err::Error(e)) => {
            println!("{:?}", trk.results());
            dump_diagnostics(txt, &e, "", true);
            return Ok(());
        }
        Err(e) => {
            println!("{:?}", e);
            return Ok(());
        }
    };

    match bcmd {
        BCommand::Index() => {
            let path = PathBuf::from(".");
            work.send.send(Msg::WalkTree(path))?;
        }
        BCommand::Find(Find::Find(v)) => {
            let mut words = data.words.lock()?;

            let find_terms = v.iter().map(|v| v.clone()).collect::<Vec<_>>();
            let found = words.find(find_terms.as_slice())?;
            let found_lines = find_matched_lines(find_terms.as_slice(), &found)?;
            for (idx, (file, lines)) in found_lines.iter().take(20).enumerate() {
                println!("  {}:{}", idx, file);
                for line in lines {
                    println!("    {}", line);
                }
            }

            let mut found_guard = data.found.lock()?;
            found_guard.terms = find_terms;
            found_guard.files = found;
            found_guard.lines_idx = 20;
            found_guard.lines = found_lines;
        }
        BCommand::Files(Files::Files(v)) => {
            let words = data.words.lock()?;
            let found = words.find_file(v.as_str());
            for (idx, file) in found.iter().enumerate() {
                println!("  {}:{}", idx, file);
            }

            let mut found_guard = data.found.lock()?;
            found_guard.terms.clear();
            found_guard.files = found;
            found_guard.lines_idx = 0;
            found_guard.lines.clear();
        }
        BCommand::Next(Next::First) => {
            let mut found_guard = data.found.lock()?;
            found_guard.lines_idx = 0;

            for (idx, (file, lines)) in found_guard
                .lines
                .iter()
                .enumerate()
                .skip(found_guard.lines_idx)
                .take(20)
            {
                println!("  {}:{}", idx, file);
                for line in lines {
                    println!("    {}", line);
                }
            }

            found_guard.lines_idx += 20;

            if found_guard.lines_idx <= found_guard.lines.len() {
                println!("...");
            }
        }
        BCommand::Next(Next::Next) => {
            let mut found_guard = data.found.lock()?;
            for (idx, (file, lines)) in found_guard
                .lines
                .iter()
                .enumerate()
                .skip(found_guard.lines_idx)
                .take(20)
            {
                println!("  {}:{}", idx, file);
                for line in lines {
                    println!("    {}", line);
                }
            }

            found_guard.lines_idx += 20;

            if found_guard.lines_idx <= found_guard.lines.len() {
                println!("...");
            }
        }
        BCommand::Summary(Summary::Files(_v)) => {}
        BCommand::Delete(Delete::Delete(v)) => {
            let words = data.words.lock()?;

            for file in words.find_file(v.as_str()) {
                work.send.send(Msg::DeleteFile(file.clone()))?;
            }
        }
        BCommand::Stats(Stats::Base) => {
            println!("send queue: {}", work.send.len());
            println!(
                "recv/send walking: {}/{}",
                work.recv_send[0].0.len(),
                work.recv_send[0].1.len()
            );
            println!(
                "recv/send loading: {}/{}",
                work.recv_send[1].0.len(),
                work.recv_send[1].1.len()
            );
            println!(
                "recv/send indexing: {}/{}",
                work.recv_send[2].0.len(),
                work.recv_send[2].1.len()
            );
            println!(
                "recv/send merge words: {}/{}",
                work.recv_send[3].0.len(),
                work.recv_send[3].1.len()
            );
            println!("recv terminal: {}", work.recv.len());

            for i in 0..8 {
                let w = &work.workers[i];
                let s = w.state.lock().unwrap();
                println!(
                    "thread[{}]: {} state={} msg={} thread={}",
                    i,
                    w.name,
                    s.state,
                    s.msg,
                    if w.handle.is_finished() {
                        "finished"
                    } else {
                        "running"
                    }
                );
            }

            let words = data.words.lock()?;
            println!("words: {}", words.words().len());
            println!("files: {}", words.files().len());

            work.send.send(Msg::Debug)?;
        }
        BCommand::Stats(Stats::Word(txt)) => {
            let block_nr = txt.parse::<u32>()?;
            let mut words = data.words.lock()?;
            let block = words.db.get(LogicalNr(block_nr))?;

            println!("{:2?}", block);
        }
        BCommand::Stats(Stats::Debug) => {
            let words = data.words.lock()?;

            let mut log = data.log.try_clone()?;
            writeln!(log, "{:#?}", *words)?;
            for (word, data) in words.words().iter() {
                writeln!(log, "{}: [{}] n={}", word, data.id, data.count)?;
            }
        }
        BCommand::Store() => {
            work.send.send(Msg::AutoSave)?;
        }
        BCommand::None => {
            //
        }
        BCommand::Help => {
            eprintln!(
                "
index
stats base | debug | <word>
find <match>
files <match>
summary <nr>
delete <file-match>
store
help | ?
"
            );
        }
    }

    Ok(())
}
