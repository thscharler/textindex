use crate::index::{index_txt, Words};
use crate::AppState;
use crossbeam::channel::bounded;
use crossbeam::channel::internal::SelectHandle;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::thread;
use std::thread::scope;
use walkdir::WalkDir;

// let (s, r) = bounded(0);
//
// scope(|scope| {
// // Spawn a thread that receives a message and then sends one.
// scope.spawn(|_| {
// r.recv().unwrap();
// s.send(2).unwrap();
// });
//
// // Send a message and then receive one.
// s.send(1).unwrap();
// r.recv().unwrap();
// }).unwrap();

pub fn update_index(data: &mut AppState, p: &Path) -> Result<(), anyhow::Error> {
    let mut words = Words::new();

    enum Msg {
        Quit(),
        Path(PathBuf, PathBuf),
        Words(Words),
    }

    scope(|scope| {
        let t = thread::available_parallelism()?.get();

        let (send_job, recv_job) = bounded::<Msg>(1024);
        let (send_res, recv_res) = bounded::<Msg>(1024);

        for _ in 0..t {
            scope.spawn(|| {
                let recv = recv_job.clone();
                let send = send_res.clone();

                let mut buf = Vec::new();
                while let Ok(msg) = recv.recv() {
                    match msg {
                        Msg::Quit() => {
                            return;
                        }
                        Msg::Path(absolute, relative) => {
                            println!("Index {:?}", relative);
                            buf.clear();

                            File::open(absolute)?.read_to_end(&mut buf)?;

                            let w = index_txt(&absolute, &buf);

                            send.send(Msg::Words(w))?;
                        }
                        Msg::Words(_) => {
                            panic!();
                        }
                    }
                }
                panic!();
            })
        }

        scope.spawn(|| {
            let recv = recv_res.clone();
            while let Ok(msg) = recv.recv() {
                match msg {}
            }
        });

        if p.exists() && p.is_dir() {
            for entry in WalkDir::new(p).into_iter().flatten() {
                if entry.metadata()?.is_file() {
                    let absolute = entry.path();
                    let relative = entry.path().strip_prefix(p)?;

                    send_job.send(Msg::Path(absolute.into(), relative.into()));

                    let w = index_txt(absolute)?;
                    data.words.merge(w);
                }
            }
        }

        send_job.is_empty()
    });

    if p.exists() && p.is_dir() {
        for entry in WalkDir::new(p).into_iter().flatten() {
            if entry.metadata()?.is_dir() {
                // let absolute = entry.path();
                // let relative = entry.path().strip_prefix(p)?;
            } else {
                let absolute = entry.path();
                let relative = entry.path().strip_prefix(p)?;

                println!("Index {:?}", relative);
                buf.clear();
                File::open(absolute)?.read_to_end(&mut buf)?;

                let w = index_txt(absolute)?;
                data.words.merge(w);
            }
        }
    }

    println!("{} files", data.words.files.len());
    println!("{} words", data.words.words.len());

    let mut wcnt = data
        .words
        .word_count
        .iter()
        .enumerate()
        .map(|v| (*v.1, v.0))
        .collect::<Vec<_>>();
    wcnt.sort();
    for (cnt, word_idx) in wcnt {
        println!("{}|{}", data.words.words[word_idx], cnt);
    }

    Ok(())
}
