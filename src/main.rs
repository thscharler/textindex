extern crate core;

use crate::cmds::{parse_cmds, BCommand, CCode, Cmds, Delete, Next, Stats, Summary};
use crate::cmds::{Files, Find};
use crate::error::AppError;
use crate::log::dump_diagnostics;
use crate::proc3::threads::{init_work, Msg, Work};
use crate::proc3::{
    auto_save, find_matched_lines, indexing, load_file, shut_down, Data, FileFilter,
};
use blockfile2::LogicalNr;
use kparse::prelude::*;
use kparse::Track;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::exit;

mod cmdlib;
mod cmds;
mod error;
// mod index;
pub mod index2;
mod log;
pub mod proc3;

fn main() -> Result<(), AppError> {
    let tmp = PathBuf::from(".tmp_stored");
    if tmp.exists() {
        fs::remove_file(tmp)?;
    }

    println!("loading");
    let stored = PathBuf::from(".stored");
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
        BCommand::Summary(Summary::Files(v)) => {
            let found_guard = data.found.lock().expect("found");
            if let Some(file) = found_guard.files.get(v) {
                let path = PathBuf::from(".");
                let path = path.join(file);

                let (filter, txt) = load_file(FileFilter::Inspect, &path)?;
                let (_, words) = indexing(filter, file, &txt)?;
                let occurance = words.invert();

                for (k, v) in occurance.iter().rev() {
                    println!("{}:", k);
                    for s in v {
                        print!("{} ", s);
                    }
                    println!();
                }
            } else {
                println!("Invalid index {}", v);
            }
        }
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
