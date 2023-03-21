use crate::cmdlib::CParserResult;
use crate::cmds::{parse_cmds, BCommand, CCode, Cmds, Delete, Stats};
use crate::cmds::{Files, Find};
use crate::error::AppError;
use crate::index::Words;
use crate::proc2::{autosave, init_work, shut_down, spin_up, Data, Msg, Work};
use kparse::prelude::*;
use kparse::Track;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};
use wildmatch::WildMatch;

mod cmdlib;
mod cmds;
mod error;
mod index;
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
            Ok(txt_input) => {
                break_flag = false;
                rl.add_history_entry(txt_input.as_str())?;
                match parse_cmd(data, work, &txt_input, &mut rl) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("parse_cmd {:?}", e);
                    }
                }
            }
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

    match parse_cmds(span) {
        Ok((_, BCommand::Index())) => {
            let path = PathBuf::from(".");
            work.send.send(Msg::Walk(path))?;
        }
        Ok((_, BCommand::Find(Find::Find(fval)))) => {
            let rd = data.words.read()?;

            let fmatch = fval
                .into_iter()
                .map(|v| WildMatch::new(v.as_str()))
                .collect::<Vec<_>>();

            let find_match = move |txt: &&String| {
                for f in &fmatch {
                    if !f.matches(txt.as_str()) {
                        return false;
                    }
                }
                return true;
            };

            for (txt, word) in rd.words.iter().filter(|(txt, _)| find_match(txt)) {
                println!("    {} {} {:?}", txt, word.count, word.file_idx);
                for f_idx in &word.file_idx {
                    println!("         {}", rd.files[*f_idx as usize]);
                }
            }
        }
        Ok((_, BCommand::Files(Files::Files(fval)))) => {
            let rd = data.words.read()?;

            let find = WildMatch::new(fval.as_str());

            for file in rd.files.iter().filter(|v| find.matches(v.as_str())) {
                println!("    {}", file);
            }
        }
        Ok((_, BCommand::Delete(Delete::Delete(fval)))) => {
            *data.modified.lock()? = true;

            let rd = data.words.read()?;

            let find = WildMatch::new(fval.as_str());

            for file in rd.files.iter().filter(|v| find.matches(v.as_str())) {
                work.send.send(Msg::DeleteFile(file.clone()))?;
            }
        }
        Ok((_, BCommand::Stats(Stats::Base))) => {
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
        Ok((_, BCommand::Stats(Stats::Debug))) => {
            let rd = data.words.read()?;

            println!("{:?}", *rd);
        }

        Ok((_, BCommand::Store())) => {
            work.send.send(Msg::AutoSave())?;
        }

        Ok((_, BCommand::None)) => {
            //
        }
        Ok((_, BCommand::Help())) => {
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
        Err(e) => {
            eprintln!("{:?}", trk.results());
            eprintln!("parse_cmds {:?}", e);
        }
    }

    Ok(())
}
