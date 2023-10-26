use crate::cmds::{parse_cmds, BCommand, CCode, Cmds, Delete, Stats};
use crate::cmds::{Files, Find};
use crate::error::AppError;
use crate::index2::Words;
use crate::log::dump_diagnostics;
use crate::proc3::{auto_save, init_work, shut_down, Data, Msg, Work};
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
// mod index;
pub mod index2;
mod log;
pub mod proc3;
pub mod tmp_index;

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
                words: RwLock::new(Words::new(&stored)?),
                modified: Mutex::new(false),
            }))
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
        let _ = auto_save(&work.printer.clone(), data);
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
            work.send.send(Msg::WalkTree(path))?;
        }
        BCommand::Find(Find::Find(v)) => {
            let mut words = data.words.write()?;

            let v = v.iter().map(|v| v.as_str()).collect::<Vec<_>>();
            for ff in words.find(v.as_slice())? {
                println!("         {}", ff);
            }
        }
        BCommand::Files(Files::Files(v)) => {
            let words = data.words.read()?;

            for file in words.find_file(v.as_str()) {
                println!("    {}", file);
            }
        }
        BCommand::Delete(Delete::Delete(v)) => {
            *data.modified.lock()? = true;

            let words = data.words.read()?;

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

            work.send.send(Msg::Debug)?;
        }
        BCommand::Stats(Stats::Debug) => {
            let rd = data.words.read()?;
            println!("{:?}", *rd);
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
