use crate::cmds::{parse_cmds, BCommand, CCode, Cmds, Delete, Stats};
use crate::cmds::{Files, Find};
use crate::error::AppError;
use crate::index::Words;
use crate::log::dump_diagnostics;
use crate::proc2::{autosave, init_work, shut_down, spin_up, Data, Msg, Work};
use kparse::prelude::*;
use kparse::Track;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};
use wildmatch::WildMatch;

mod cmdlib;
mod cmdlib2;
mod cmds;
mod error;
mod index;
mod log;
mod proc2;

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
            println!("start with empty index");
            Box::leak(Box::new(Data {
                words: RwLock::new(Words::new()),
                modified: Mutex::new(false),
            }))
        }
    };

    let mut rl = Editor::<Cmds, FileHistory>::new()?;
    rl.set_helper(Some(Cmds));
    let _ = rl.load_history("history.txt");

    println!("spinup");
    let work: &'static Work = Box::leak(Box::new(init_work(rl.create_external_printer()?)));
    spin_up(work, data);

    let mut break_flag = false;
    loop {
        match rl.readline("> ") {
            Ok(txt_input) if txt_input.len() > 0 => {
                break_flag = false;
                rl.add_history_entry(txt_input.as_str())?;
                match parse_cmd(data, work, &txt_input, &mut rl) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("parse_cmd {:?}", e);
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

    if *data.modified.lock()? {
        let _ = autosave(data);
    }

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
            work.send.send(Msg::Walk(path))?;
        }
        BCommand::Find(Find::Find(fval)) => {
            let rd = data.words.read()?;

            let mut first = true;
            let mut collect_idx = BTreeSet::new();
            for fval in fval {
                let find = WildMatch::new(fval.as_str());

                let mut f_idx = BTreeSet::new();
                for (_txt, word) in rd.words.iter().filter(|(txt, _)| find.matches(txt)) {
                    for ff in word.file_idx.iter() {
                        if first {
                            f_idx.insert(*ff);
                        } else {
                            if collect_idx.contains(ff) {
                                f_idx.insert(*ff);
                            }
                        }
                    }
                }

                first = false;
                collect_idx = f_idx;
            }

            for ff in collect_idx {
                println!("         {}", rd.files[ff as usize]);
            }
        }
        BCommand::Files(Files::Files(fval)) => {
            let rd = data.words.read()?;

            let find = WildMatch::new(fval.as_str());
            for file in rd.files.iter().filter(|v| find.matches(v.as_str())) {
                println!("    {}", file);
            }
        }
        BCommand::Delete(Delete::Delete(fval)) => {
            *data.modified.lock()? = true;

            let rd = data.words.read()?;

            let find = WildMatch::new(fval.as_str());
            for file in rd.files.iter().filter(|v| find.matches(v.as_str())) {
                work.send.send(Msg::DeleteFile(file.clone()))?;
            }
        }
        BCommand::Stats(Stats::Base) => {
            let rd = data.words.read()?;

            println!("send queue: {}", work.send.len());
            let mut t_cnt = 0;
            let mut t_fine = 0;
            for h in work.handles.borrow().iter() {
                if !h.is_finished() {
                    t_fine += 1;
                }
                t_cnt += 1;
            }
            println!("threads: {}/{}", t_fine, t_cnt);

            println!("files: {}", rd.files.len());
            println!("words: {}", rd.words.len());
        }
        BCommand::Stats(Stats::Debug) => {
            let rd = data.words.read()?;
            println!("{:?}", *rd);
        }
        BCommand::Store() => {
            work.send.send(Msg::AutoSave())?;
        }
        BCommand::None => {
            //
        }
        BCommand::Help => {
            eprintln!(
                "
index
stats base | debug
find <match>
files <match>
delete <file-match>
help | ?
"
            );
        }
    }

    Ok(())
}
