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
            *data.modified.lock()? = true;

            let path = PathBuf::from(".");
            work.send.send(Msg::Walk(path))?;
        }
        Ok((_, BCommand::Find(Find::Find(fval)))) => {
            let rd = data.words.read()?;

            for (idx, str) in rd.words.iter().enumerate().filter(|v| v.1.contains(&fval)) {
                println!("    {} {}", str, rd.word_count[idx]);
                for f_idx in &rd.file_idx[idx] {
                    println!("         {}", rd.files[*f_idx]);
                }
            }
        }
        Ok((_, BCommand::Files(Files::Files(fval)))) => {
            let rd = data.words.read()?;

            for file in rd.files.iter().filter(|v| v.contains(&fval)) {
                println!("    {}", file);
            }
        }
        Ok((_, BCommand::Delete(Delete::Delete(fval)))) => {
            *data.modified.lock()? = true;

            let rd = data.words.read()?;

            for file in rd.files.iter().filter(|v| v.contains(&fval)) {
                work.send.send(Msg::DeleteFile(file.clone()))?;
            }
        }
        Ok((_, BCommand::Stats(Stats::Base))) => {
            let rd = data.words.read()?;

            println!("send queue: {}", work.send.len());
            println!("files: {}", rd.files.len());
            println!("words: {}", rd.words.len());
        }

        // let mut top_ten: Vec<(&str, usize)> = Vec::new();
        // for (idx, count) in rd.word_count.iter().enumerate() {
        //     let mut ins = false;
        //     for i in 0..top_ten.len() {
        //         let t10_count = top_ten[i].1;
        //         if *count > t10_count {
        //             let str = rd.words[idx].as_str();
        //             top_ten.insert(i, (str, *count));
        //             ins = true;
        //             break;
        //         }
        //     }
        //     if !ins && top_ten.len() < 10 {
        //         let str = rd.words[idx].as_str();
        //         top_ten.push((str, *count));
        //     }
        // }
        // println!("top ten:");
        // for (t10_str, t10_count) in top_ten.iter() {
        //     println!("    {} : {}", t10_str, t10_count);
        // }
        Ok((_, BCommand::None)) => {
            //
        }
        Ok((_, BCommand::Help())) => {
            eprintln!(
                "
index
stats
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
