use crate::error::AppError;
use crate::index::{index_html, index_txt, Words};
use crossbeam::channel::{unbounded, Receiver, Sender};
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};
use std::{fs, mem, thread};
use walkdir::WalkDir;

#[derive(Debug)]
pub enum Msg {
    Quit(),
    Load(FileFilter, PathBuf, String),
    Index(FileFilter, PathBuf, String, String),
    DeleteFile(String),
    Walk(PathBuf),
    Words(Words),
    AutoSave(),
}

#[derive(Debug, PartialEq, Eq)]
pub enum FileFilter {
    Ignore,
    Inspect,
    Text,
    Html,
}

pub struct Data {
    pub words: RwLock<Words>,
    pub modified: Mutex<bool>,
}

impl Data {
    pub fn write(&'static self, path: &Path) -> Result<(), AppError> {
        let rdl = self.words.read()?;
        rdl.write(path)
    }

    pub fn read(path: &Path) -> Result<&'static Data, AppError> {
        let words = Words::read(path)?;

        let data: &'static Data = Box::leak(Box::new(Data {
            words: RwLock::new(words),
            modified: Mutex::new(false),
        }));

        Ok(data)
    }
}

pub struct Work {
    pub nthreads: u32,

    pub send: Sender<Msg>,
    pub recv: Receiver<Msg>,

    pub printer: Arc<Mutex<dyn ExternalPrinter + Send + Sync>>,

    pub handles: RefCell<Vec<JoinHandle<()>>>,
}

pub fn init_work<P: ExternalPrinter + Send + Sync + 'static>(print: P) -> Work {
    let t = thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(2)
        + 1;

    let (sw, rw) = unbounded::<Msg>();

    Work {
        nthreads: t as u32,
        send: sw,
        recv: rw,
        printer: Arc::new(Mutex::new(print)),
        handles: RefCell::new(Vec::new()),
    }
}

pub fn shut_down(work: &Work) {
    println!("sending shutdown");
    for _ in 0..work.nthreads {
        if let Err(e) = work.send.send(Msg::Quit()) {
            if let Ok(mut print) = work.printer.lock() {
                let _ = print.print(format!("shutdown {:?}", e));
            }
        }
    }

    loop {
        if let Ok(mut print) = work.printer.lock() {
            let _ = print.print("wait on shutdown".into());
        }

        sleep(Duration::from_millis(100));

        for h in work.handles.borrow().iter() {
            if !h.is_finished() {
                continue;
            }
        }

        break;
    }
}

pub fn spin_up(work: &'static Work, data: &'static Data) {
    for _ in 0..work.nthreads {
        let recv = work.recv.clone();
        let send = work.send.clone();
        let printer = work.printer.clone();

        let h = thread::spawn(move || {
            let recv = recv;
            let send = send;
            let data = data;
            let printer = printer;

            while let Ok(msg) = recv.recv() {
                match msg {
                    Msg::Quit() => {
                        break;
                    }
                    msg => {
                        let dis = mem::discriminant(&msg);
                        if let Err(e) = proc_msg(msg, data, &send) {
                            if let Ok(mut print) = printer.lock() {
                                let _ = print.print(format!("work {:#?} {:?}", dis, e));
                            }
                        }
                    }
                }
            }
        });

        work.handles.borrow_mut().push(h);
    }
}

fn proc_msg(msg: Msg, data: &'static Data, send: &Sender<Msg>) -> Result<(), AppError> {
    match msg {
        Msg::Quit() => {}
        Msg::Index(filter, absolute, relative, txt) => {
            timing("indexing", move || {
                indexing(filter, absolute, relative, txt, send)
            })?;
        }
        Msg::Load(filter, absolute, relative) => {
            timing("loading", move || loading(filter, absolute, relative, send))?;
        }
        Msg::Walk(path) => {
            timing("walking", move || walking(&path, data, &send))?;
        }
        Msg::Words(words) => {
            timing("merging", move || merging(words, data, &send))?;
        }
        Msg::AutoSave() => {
            timing("autosave", || autosave(data))?;
        }
        Msg::DeleteFile(file) => {
            timing("deleting", move || deleting(data, file))?;
        }
    }

    Ok(())
}

pub fn loading(
    filter: FileFilter,
    absolute: PathBuf,
    relative: String,
    send: &Sender<Msg>,
) -> Result<(), AppError> {
    //
    let mut buf = Vec::new();
    File::open(&absolute)?.read_to_end(&mut buf)?;
    let str = String::from_utf8_lossy(buf.as_slice());

    let filter = content_filter(filter, str.as_ref());

    if filter != FileFilter::Ignore {
        send.send(Msg::Index(filter, absolute, relative, str.into()))?;
    }

    Ok(())
}

pub fn deleting(data: &'static Data, file: String) -> Result<(), AppError> {
    let mut write = data.words.write()?;
    write.remove_file(file);

    Ok(())
}

pub fn autosave(data: &'static Data) -> Result<(), AppError> {
    let tmp = Path::new(".tmp_stored");
    if tmp.exists() {
        return Ok(());
    }

    data.write(tmp)?;

    fs::rename(tmp, Path::new(".stored"))?;

    Ok(())
}

fn merging(words: Words, data: &'static Data, send: &Sender<Msg>) -> Result<(), AppError> {
    let mut auto_save = false;

    {
        let mut write = data.words.write()?;
        write.append(words);

        if write.age.elapsed() > Duration::from_secs(60) {
            write.age = Instant::now();
            auto_save = true;
        }
    }

    if auto_save {
        send.send(Msg::AutoSave())?;
    }

    Ok(())
}

fn walking(path: &Path, data: &'static Data, send: &Sender<Msg>) -> Result<(), AppError> {
    if !path.exists() || !path.is_dir() {
        return Ok(());
    }

    // only need the initial set for the check.
    let chk_files = {
        let read = data.words.read()?;
        read.files.iter().cloned().collect::<HashSet<_>>()
    };

    for entry in WalkDir::new(path).into_iter().flatten() {
        let meta = entry.metadata()?;
        if meta.is_file() {
            let absolute = entry.path();
            let relative = entry
                .path()
                .strip_prefix(path)
                .unwrap_or(absolute)
                .to_string_lossy()
                .to_string();

            let filter = name_filter(&absolute);

            if filter == FileFilter::Ignore {
                println!("ignore {:?}", relative);
                continue;
            }

            // avoid flooding
            while send.len() > 128 {
                sleep(Duration::from_secs(1))
            }

            if !chk_files.contains(&relative) {
                send.send(Msg::Load(filter, absolute.into(), relative))?;
            } else {
                println!("seen {:?}", relative);
            }
        }
    }

    Ok(())
}

fn indexing(
    filter: FileFilter,
    abs_path: PathBuf,
    rel_path: String,
    txt: String,
    send: &Sender<Msg>,
) -> Result<(), AppError> {
    //
    let mut words = Words::new();

    match filter {
        FileFilter::Text => {
            println!("index {:?}", abs_path);
            let file_idx = words.add_file(rel_path);
            index_txt(&mut words, file_idx, &txt);
        }
        FileFilter::Html => {
            println!("index {:?}", abs_path);
            let file_idx = words.add_file(rel_path);
            index_html(&mut words, file_idx, &txt)?;
        }
        FileFilter::Ignore => {}
        FileFilter::Inspect => {}
    }

    send.send(Msg::Words(words))?;

    Ok(())
}

fn content_filter(filter: FileFilter, txt: &str) -> FileFilter {
    if filter != FileFilter::Inspect {
        return filter;
    }

    if txt.starts_with("<?xml") || txt.starts_with("<!DOCTYPE") || txt.starts_with("<html") {
        FileFilter::Html
    } else {
        FileFilter::Ignore
    }
}

fn name_filter(path: &Path) -> FileFilter {
    let ext = path
        .extension()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""))
        .to_lowercase();
    let name = path
        .file_name()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""))
        .to_lowercase();

    if name == ".message.ftp.txt"
        || name == "history.txt"
        || name == ".stored"
        || name == ".tmp_stored"
    {
        FileFilter::Ignore
    } else if ext == "jpg" || ext == "pdf" || ext == "gif" || ext == "css" || ext == "png" {
        FileFilter::Ignore
    } else if ext == "doc" {
        FileFilter::Ignore
    } else if ext == "rtf" {
        FileFilter::Ignore
    } else if ext == "html" || ext == "htm" {
        FileFilter::Html
    } else if ext == "txt" {
        FileFilter::Text
    } else {
        FileFilter::Inspect
    }
}

pub fn timing<R>(name: &str, mut fun: impl FnOnce() -> R) -> R {
    let now = Instant::now();
    let result = fun();
    println!("{} {:?}", name, now.elapsed());
    result
}
