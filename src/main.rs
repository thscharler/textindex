use crate::cmds::Find;
use crate::cmds::{parse_cmds, BCommand, CCode, Cmds};
use crate::error::AppError;
use crate::index::Words;
use crate::proc2::{autosave, init_work, shut_down, spin_up, Data, Msg, Work};
use kparse::Track;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;
use std::fs;
use std::path::PathBuf;
use std::sync::RwLock;

mod cmdlib;
mod cmds;
mod error;
mod index;
mod proc2;

fn main() -> Result<(), AppError> {
    let tmp = PathBuf::from(".tmp_stored");
    fs::remove_file(tmp)?;

    println!("loading");
    let stored = PathBuf::from(".stored");
    let data = match Data::read(&stored) {
        Ok(v) => v,
        Err(e) => {
            println!("{:?}", e);
            Box::leak(Box::new(Data {
                words: RwLock::new(Words::new()),
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
                    Err(e) => eprintln!("parse_cmd {:?}", e),
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
    let _ = autosave(data);
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
        Ok((_, BCommand::Index(_))) => {
            let path = PathBuf::from(".");
            work.send.send(Msg::Walk(path))?;
        }
        Ok((_, BCommand::Find(Find::Find(fval)))) => {
            println!("sendq {}", work.send.len());

            let rd = data.words.read()?;

            println!("{} files", rd.files.len());
            println!("{} words", rd.words.len());

            for (val, count) in rd
                .words
                .iter()
                .enumerate()
                .filter(|(_idx, val)| val.starts_with(&fval))
                .map(|(idx, val)| (val, rd.word_count[idx]))
            {
                println!("{} | {}", val, count);
            }

            // let mut wcnt = rd
            //     .word_count
            //     .iter()
            //     .filter(|v| **v < 2)
            //     .enumerate()
            //     .map(|(idx, count)| (rd.words[idx].as_str(), count))
            //     .filter(|(txt, count)| txt.chars().find(|c| !c.is_alphanumeric()).is_some())
            //     .collect::<Vec<_>>();
            // wcnt.sort();
            // for (word, count) in wcnt {
            //     println!("{} | {}", word, count);
            // }
        }
        Ok((_, BCommand::None)) => {
            //
        }
        Ok((_, BCommand::Help(v))) => {
            eprintln!(
                "
index

find text <txt>

?
"
            );
            dbg!(v);
        }
        Err(e) => {
            eprintln!("parse_cmds {:?}", e);
        }
    }

    Ok(())
}
