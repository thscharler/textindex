use crate::cmdlib::{CParserResult, CSpan};
use crate::cmds::{parse_cmds, BCommand, CCode, Cmds};
use crate::index::Words;
use kparse::prelude::*;
use kparse::Track;
use rustyline::error::ReadlineError;
use rustyline::history::FileHistory;
use rustyline::Editor;

mod cmdlib;
mod cmds;
mod index;

fn main() -> Result<(), anyhow::Error> {
    let mut data = Words::new();

    let mut rl = Editor::<Cmds, FileHistory>::new()?;
    let _ = rl.load_history("history.txt");

    let mut break_flag = false;
    loop {
        match rl.readline("> ") {
            Ok(txt_input) => {
                break_flag = false;
                rl.add_history_entry(txt_input.as_str())?;
                match parse_cmd(&mut data, &txt_input, &mut rl) {
                    Ok(_) => {}
                    Err(e) => eprintln!("{:?}", e),
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
                eprintln!("{:?}", err);
            }
        }
    }

    rl.save_history("history.txt")?;

    Ok(())
}

fn parse_cmd(
    _data: &mut Words,
    txt: &str,
    _rl: &mut Editor<Cmds, FileHistory>,
) -> Result<(), anyhow::Error> {
    let trk = Track::new_tracker::<CCode, _>();
    let span = Track::new_span(&trk, txt);

    match parse_cmds(span) {
        Ok((_, BCommand::Index(v))) => {
            dbg!(v);
        }
        Ok((_, BCommand::Find(v))) => {
            dbg!(v);
        }
        Ok((_, BCommand::None)) => {
            dbg!(());
        }
        Ok((_, BCommand::Help(v))) => {
            dbg!(v);
        }
        Err(e) => {
            eprintln!("{:?}", e);
        }
    }

    Ok(())
}
