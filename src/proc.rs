use crate::index::{index_html, index_txt, Words};
use crate::AppState;
use crossbeam::channel::{bounded, TryRecvError};
use std::borrow::Cow;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::{from_utf8, from_utf8_unchecked, Utf8Error};
use std::thread;
use std::thread::{scope, sleep};
use std::time::Duration;
use walkdir::WalkDir;

pub fn update_index(data: &mut AppState, p: &Path) -> Result<(), anyhow::Error> {
    let mut words = Words::new();

    enum Msg {
        Quit(),
        Path(PathBuf, PathBuf),
        Words(Words),
    }

    scope(|scope| {
        let t = thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(2);

        let (send_job, recv_job) = bounded::<Msg>(1024);
        let (send_res, recv_res) = bounded::<Msg>(t);

        let mut handles = Vec::new();
        for _ in 0..t {
            let recv = recv_job.clone();
            let send = send_res.clone();

            let handle = scope.spawn(move || {
                let recv = recv;
                let send = send;

                let mut words = Words::new();
                let mut buf = Vec::new();
                while let Ok(msg) = recv.recv() {
                    match msg {
                        Msg::Quit() => {
                            if let Err(e) = send.send(Msg::Words(words)) {
                                eprintln!("ERR1 {:?}", e)
                            }
                            return;
                        }
                        Msg::Path(absolute, relative) => {
                            // println!("Index {:?}", relative);

                            buf.clear();
                            let mut f = match File::open(&absolute) {
                                Ok(v) => v,
                                Err(e) => {
                                    eprintln!("ERR2 {:?}", e);
                                    continue;
                                }
                            };
                            match f.read_to_end(&mut buf) {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("ERR3 {:?}", e);
                                    continue;
                                }
                            }
                            let str = match from_utf8(buf.as_slice()) {
                                Ok(v) => v,
                                Err(_) => {
                                    let _ = buf.iter_mut().map(|v| {
                                        if *v > 127 {
                                            *v = b'_';
                                        }
                                    });

                                    match from_utf8(buf.as_slice()) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            eprintln!("ERR9 {:?}: {:?}", absolute, e);
                                            continue;
                                        }
                                    }
                                }
                            };

                            let file_idx = words.add_file(relative);

                            let ext = absolute
                                .extension()
                                .map(|v| v.to_string_lossy())
                                .unwrap_or(Cow::Borrowed(""));
                            if ext == "html"
                                || str.starts_with("<?xml")
                                || str.starts_with("<!DOCTYPE")
                                || str.starts_with("<html")
                            {
                                match index_html(&mut words, file_idx, &str) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        eprintln!("ERR4 {:?}", e);
                                        continue;
                                    }
                                }
                            } else {
                                index_txt(&mut words, file_idx, &str)
                            };
                        }
                        _ => {}
                    }
                }
                panic!("recv failed");
            });

            handles.push(handle);
        }

        if p.exists() && p.is_dir() {
            for entry in WalkDir::new(p).into_iter().flatten() {
                match entry.metadata() {
                    Ok(v) if v.is_file() => {
                        let absolute = entry.path();
                        let relative = entry.path().strip_prefix(p).unwrap_or(absolute);

                        if let Err(e) = send_job.send(Msg::Path(absolute.into(), relative.into())) {
                            eprintln!("ERR5 {:?}", e);
                        };
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("ERR6 {:?}", e);
                    }
                }
            }
        }

        for _ in 0..t {
            if let Err(e) = send_job.send(Msg::Quit()) {
                eprintln!("ERR7 {:?}", e);
            }
        }

        'collect: loop {
            sleep(Duration::from_millis(1000));

            match recv_res.try_recv() {
                Ok(v) => match v {
                    Msg::Words(v) => {
                        words.merge(v);
                    }
                    _ => {}
                },
                Err(e) => match e {
                    TryRecvError::Empty => {}
                    TryRecvError::Disconnected => {
                        eprintln!("ERR8 {:?}", e);
                    }
                },
            }

            for h in &handles {
                if !h.is_finished() {
                    continue 'collect;
                }
            }

            break 'collect;
        }
    });

    data.words = words;

    println!("{} files", data.words.files.len());
    println!("{} words", data.words.words.len());

    let mut wcnt = data
        .words
        .word_count
        .iter()
        .filter(|v| **v > 1000)
        .enumerate()
        .map(|v| (*v.1, v.0))
        .collect::<Vec<_>>();
    wcnt.sort();
    for (cnt, word_idx) in wcnt {
        println!("{}|{}", data.words.words[word_idx], cnt);
    }

    Ok(())
}
