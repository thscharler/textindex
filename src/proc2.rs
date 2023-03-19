use crate::error::AppError;
use crate::index::{index_html, index_txt, Words};
use crossbeam::channel::{unbounded, Receiver, Sender};
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};
use std::{fs, mem, thread};
use walkdir::WalkDir;

#[derive(Debug)]
pub enum Msg {
    Quit(),
    Index(PathBuf, String),
    Walk(PathBuf),
    Words(Words),
    AutoSave(),
}

pub struct Data {
    pub words: RwLock<Words>,
}

impl Data {
    pub fn write(&'static self, path: &Path) -> Result<(), AppError> {
        let read = self.words.read()?;

        let mut f = BufWriter::new(File::create(path)?);

        f.write_all(&(read.files.len() as u32).to_ne_bytes())?;
        for file in read.files.iter() {
            f.write_all(file.as_bytes())?;
            f.write_all(&[0])?;
        }

        f.write_all(&(read.words.len() as u32).to_ne_bytes())?;
        for ((word, count), idx) in (read.words.iter())
            .zip(read.word_count.iter())
            .zip(read.file_idx.iter())
        {
            f.write_all(word.as_bytes())?;
            f.write_all(&[0])?;
            f.write_all(&(*count as u32).to_ne_bytes())?;

            f.write_all(&(idx.len() as u32).to_ne_bytes())?;
            for u in idx {
                f.write_all(&(*u as u32).to_ne_bytes())?;
            }
        }

        Ok(())
    }

    pub fn read(path: &Path) -> Result<&'static Data, AppError> {
        let data: &'static Data = Box::leak(Box::new(Data {
            words: RwLock::new(Words::new()),
        }));
        let mut write = data.words.write()?;

        let mut f = BufReader::new(File::open(path)?);
        let mut buf = Vec::new();
        let mut u = [0u8; 4];

        f.read_exact(&mut u)?;
        let n = u32::from_ne_bytes(u) as usize;
        for _ in 0..n {
            buf.clear();

            f.read_until(b'\0', &mut buf)?;
            buf.pop();
            let file = from_utf8(&buf)?.to_string();
            write.files.push(file);
        }

        f.read_exact(&mut u)?;
        let n = u32::from_ne_bytes(u) as usize;
        for _ in 0..n {
            buf.clear();

            f.read_until(b'\0', &mut buf)?;
            buf.pop();
            let word = from_utf8(&buf)?.to_string();
            write.words.push(word);

            f.read_exact(&mut u)?;
            let count = u32::from_ne_bytes(u);
            write.word_count.push(count as usize);

            let mut file_idx = HashSet::new();
            f.read_exact(&mut u)?;
            let n = u32::from_ne_bytes(u);
            for _ in 0..n {
                f.read_exact(&mut u)?;
                let idx = u32::from_ne_bytes(u);
                file_idx.insert(idx as usize);
            }
            write.file_idx.push(file_idx);
        }

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
        * 4;

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
        Msg::Index(absolute, relative) => {
            indexing(absolute, relative, send)?;
        }
        Msg::Walk(path) => {
            walking(&path, data, &send)?;
        }
        Msg::Words(words) => {
            merging(words, data, &send)?;
        }
        Msg::AutoSave() => {
            autosave(data)?;
        }
    }

    Ok(())
}

pub fn autosave(data: &'static Data) -> Result<(), AppError> {
    let tmp = PathBuf::from(".tmp_stored");
    let stored = PathBuf::from(".stored");

    let inst = Instant::now();

    if tmp.exists() {
        return Ok(());
    }
    data.write(&tmp)?;
    fs::rename(&tmp, &stored)?;

    println!("autosave in {:?}", Instant::now().duration_since(inst),);

    Ok(())
}

fn merging(words: Words, data: &'static Data, send: &Sender<Msg>) -> Result<(), AppError> {
    let mut write = data.words.write()?;

    let n = write.words.len();
    let m = words.words.len();

    let inst = Instant::now();

    let (upd, ins) = write.append(words);

    println!(
        "{:?} data {}/add {}  up {}/in {}",
        Instant::now().duration_since(inst),
        n,
        m,
        upd,
        ins
    );

    let now = Instant::now();
    if now.duration_since(write.age) > Duration::from_secs(60) {
        write.age = now;
        send.send(Msg::AutoSave())?;
    }

    Ok(())
}

fn walking(path: &Path, data: &'static Data, send: &Sender<Msg>) -> Result<(), AppError> {
    if !path.exists() || !path.is_dir() {
        return Ok(());
    }

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

            // avoid flooding
            while send.len() > 128 {
                sleep(Duration::from_secs(1))
            }

            let read = data.words.read()?;
            if !read.files.contains(&relative) {
                send.send(Msg::Index(absolute.into(), relative))?;
            } else {
                println!("seen {:?}", relative);
            }
        }
    }

    Ok(())
}

fn indexing(abs_path: PathBuf, rel_path: String, send: &Sender<Msg>) -> Result<(), AppError> {
    let mut words = Words::new();

    let mut buf = Vec::new();
    File::open(&abs_path)?.read_to_end(&mut buf)?;
    let str = String::from_utf8_lossy(buf.as_slice());

    let file_idx = words.add_file(rel_path);

    let ext = abs_path
        .extension()
        .map(|v| v.to_string_lossy())
        .unwrap_or(Cow::Borrowed(""));
    if ext == "jpg" {
    } else if ext == "html"
        || str.starts_with("<?xml")
        || str.starts_with("<!DOCTYPE")
        || str.starts_with("<html")
    {
        index_html(&mut words, file_idx, &str)?;
    } else {
        index_txt(&mut words, file_idx, &str);
    };

    send.send(Msg::Words(words))?;

    Ok(())
}
