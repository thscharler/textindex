mod stop_words;

use crate::error::AppError;
use crate::index2::tmp_index::TmpWords;
use crate::index2::Words;
use crate::proc3::parse::{TxtCode, TxtPart};
use crate::proc3::stop_words::STOP_WORDS;
use crossbeam::channel::{Receiver, Sender};
use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
use kparse::prelude::TrackProvider;
use kparse::Track;
use rustyline::ExternalPrinter;
use std::borrow::Cow;
use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};
use wildmatch::WildMatch;

#[derive(Debug)]
pub enum Msg {
    Quit,
    WalkTree(PathBuf),
    WalkFinished(PathBuf),
    Load(u32, FileFilter, PathBuf, String),
    Index(u32, FileFilter, PathBuf, String, Vec<u8>),
    MergeWords(u32, TmpWords),
    DeleteFile(String),
    Debug,
    AutoSave,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FileFilter {
    Ignore,
    Inspect,
    Binary,
    Text,
    Html,
}

#[derive(Default)]
pub struct Found {
    pub terms: Vec<String>,

    pub files: Vec<String>,

    pub lines_idx: usize,
    pub lines: Vec<(String, Vec<String>)>,
}

pub struct Data {
    pub words: Mutex<Words>,
    pub found: Mutex<Found>,
    pub log: File,
}

impl Data {
    pub fn write(&'static self) -> Result<(), AppError> {
        if let Ok(mut wrl) = self.words.lock() {
            wrl.write()?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn read(path: &Path) -> Result<&'static Data, AppError> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open("log.txt")?;

        let words = Words::read(path)?;

        let data: &'static Data = Box::leak(Box::new(Data {
            words: Mutex::new(words),
            found: Default::default(),
            log,
        }));

        Ok(data)
    }
}

#[derive(Default)]
pub struct WorkerState {
    pub state: u64,
    pub msg: String,
}

pub struct Worker {
    pub name: &'static str,
    pub handle: JoinHandle<()>,
    pub state: Arc<Mutex<WorkerState>>,
}

impl Worker {
    pub fn new(name: &'static str, handle: JoinHandle<()>, state: Arc<Mutex<WorkerState>>) -> Self {
        Self {
            name,
            handle,
            state,
        }
    }
}

pub struct Work {
    pub send: Sender<Msg>,
    pub recv_send: [(Receiver<Msg>, Sender<Msg>); 4],
    pub recv: Receiver<Msg>,
    pub workers: [Worker; 8],

    pub printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
}

pub mod mt {
    use crate::error::AppError;
    use crate::proc3::{
        indexing, load_file, merge_words, name_filter, print_, print_err_, terminal_proc, Data,
        FileFilter, Msg, Work, Worker, WorkerState,
    };
    use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
    use rustyline::ExternalPrinter;
    use std::io::Write;
    use std::iter::Flatten;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::thread::JoinHandle;
    use walkdir::WalkDir;

    pub fn init_work<P: ExternalPrinter + Send + Sync + 'static>(
        printer: P,
        data: &'static Data,
    ) -> Work {
        let printer = Arc::new(Mutex::new(printer));

        let (s0, r1) = bounded::<Msg>(10);
        let (s1, r2) = bounded::<Msg>(10);
        let (s2, r3) = bounded::<Msg>(10);
        let (s3, r4) = bounded::<Msg>(10);
        let (s4, r5) = bounded::<Msg>(10);

        let n1 = "walking";
        let st1 = Arc::new(Mutex::new(WorkerState::default()));
        let h1 = spawn_walking(
            r1.clone(),
            s1.clone(),
            Arc::clone(&st1),
            data,
            printer.clone(),
        );
        let n2 = "loading";
        let st2 = Arc::new(Mutex::new(WorkerState::default()));
        let h2 = spawn_loading(
            r2.clone(),
            s2.clone(),
            Arc::clone(&st2),
            data,
            printer.clone(),
        );
        let n3_1 = "index 1";
        let st3_1 = Arc::new(Mutex::new(WorkerState::default()));
        let h3_1 = spawn_indexing(
            r3.clone(),
            s3.clone(),
            Arc::clone(&st3_1),
            data,
            printer.clone(),
        );
        let n3_2 = "index 2";
        let st3_2 = Arc::new(Mutex::new(WorkerState::default()));
        let h3_2 = spawn_indexing(
            r3.clone(),
            s3.clone(),
            Arc::clone(&st3_2),
            data,
            printer.clone(),
        );
        let n3_3 = "index 3";
        let st3_3 = Arc::new(Mutex::new(WorkerState::default()));
        let h3_3 = spawn_indexing(
            r3.clone(),
            s3.clone(),
            Arc::clone(&st3_3),
            data,
            printer.clone(),
        );
        let n3_4 = "index 4";
        let st3_4 = Arc::new(Mutex::new(WorkerState::default()));
        let h3_4 = spawn_indexing(
            r3.clone(),
            s3.clone(),
            Arc::clone(&st3_4),
            data,
            printer.clone(),
        );
        let n4 = "merge";
        let st4 = Arc::new(Mutex::new(WorkerState::default()));
        let h4 = spawn_merge_words(
            r4.clone(),
            s4.clone(),
            Arc::clone(&st4),
            data,
            printer.clone(),
        );
        let n5 = "terminal";
        let st5 = Arc::new(Mutex::new(WorkerState::default()));
        let h5 = spawn_terminal(r5.clone(), Arc::clone(&st5), data, printer.clone());

        Work {
            send: s0,
            recv_send: [(r1, s1), (r2, s2), (r3, s3), (r4, s4)],
            recv: r5,
            workers: [
                Worker::new(n1, h1, st1),
                Worker::new(n2, h2, st2),
                Worker::new(n3_1, h3_1, st3_1),
                Worker::new(n3_2, h3_2, st3_2),
                Worker::new(n3_3, h3_3, st3_3),
                Worker::new(n3_4, h3_4, st3_4),
                Worker::new(n4, h4, st4),
                Worker::new(n5, h5, st5),
            ],
            printer,
        }
    }

    fn spawn_walking(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            print_err_(
                &printer,
                data.log.try_clone().unwrap(),
                "walker",
                walk_proc(recv, send, state, data, &printer),
            );
        })
    }

    fn spawn_loading(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            print_err_(
                &printer,
                data.log.try_clone().unwrap(),
                "loading",
                load_proc(recv, send, state, data, &printer),
            );
        })
    }

    fn spawn_indexing(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            print_err_(
                &printer,
                data.log.try_clone().unwrap(),
                "indexing",
                index_proc(recv, send, state, data, &printer),
            );
        })
    }

    fn spawn_merge_words(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            print_err_(
                &printer,
                data.log.try_clone().unwrap(),
                "merge_words",
                merge_words_proc(recv, send, state, data, &printer),
            )
        })
    }

    fn spawn_terminal(
        recv: Receiver<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            print_err_(
                &printer,
                data.log.try_clone().unwrap(),
                "terminal",
                terminal_proc(&recv, state, data, &printer),
            );
        })
    }

    struct WalkingProc {
        path: PathBuf,
        tree_iter: Flatten<walkdir::IntoIter>,
        count: u32,
    }

    fn walk_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        // This is a bit more complicated, as we need to keep up the message flow
        // while traversing the directory tree. We interweave each step of the tree iteration
        // and message processing.

        let mut proc = None;

        loop {
            match &mut proc {
                None => match recv.recv()? {
                    Msg::Quit => {
                        state.lock().unwrap().state = 1;
                        send.send(Msg::Quit)?;
                        break;
                    }
                    Msg::Debug => {
                        state.lock().unwrap().state = 2;
                        print_(printer, "walk_tree empty");
                        send.send(Msg::Debug)?;
                    }
                    Msg::WalkTree(path) => {
                        state.lock().unwrap().state = 3;
                        proc = Some(WalkingProc {
                            path: path.clone(),
                            tree_iter: WalkDir::new(path).into_iter().flatten(),
                            count: 0,
                        });
                    }
                    msg => {
                        state.lock().unwrap().state = 4;
                        send.send(msg)?;
                    }
                },
                Some(rproc) => {
                    match recv.try_recv() {
                        Ok(Msg::Quit) => {
                            state.lock().unwrap().state = 5;
                            send.send(Msg::Quit)?;
                            break;
                        }
                        Ok(Msg::Debug) => {
                            state.lock().unwrap().state = 6;
                            print_(printer, format!("walk_tree {}", rproc.count));
                            send.send(Msg::Debug)?;
                        }
                        Ok(Msg::WalkTree(_)) => {
                            state.lock().unwrap().state = 7;
                            if let Ok(mut print) = printer.lock() {
                                let _ = print.print(
                                    "new tree walk ignored, still working on the last one."
                                        .to_string(),
                                );
                            }
                        }
                        Ok(msg) => {
                            state.lock().unwrap().state = 8;
                            send.send(msg)?;
                        }
                        Err(TryRecvError::Empty) => {
                            state.lock().unwrap().state = 9;
                        }
                        Err(TryRecvError::Disconnected) => {
                            state.lock().unwrap().state = 10;
                            break;
                        }
                    }

                    if let Some(entry) = rproc.tree_iter.next() {
                        state.lock().unwrap().state = 101;
                        let meta = entry.metadata()?;
                        if meta.is_file() {
                            let absolute = entry.path();
                            let relative = entry
                                .path()
                                .strip_prefix(&rproc.path)
                                .unwrap_or(absolute)
                                .to_string_lossy()
                                .to_string();

                            let filter = name_filter(absolute);

                            if filter == FileFilter::Ignore {
                                continue;
                            }

                            let do_send = {
                                state.lock().unwrap().state = 102;
                                let words = data.words.lock()?;
                                !words.have_file(&relative)
                            };
                            if do_send {
                                state.lock().unwrap().state = 103;
                                rproc.count += 1;
                                send.send(Msg::Load(
                                    rproc.count,
                                    filter,
                                    absolute.into(),
                                    relative,
                                ))?;
                            }
                        }
                    } else {
                        state.lock().unwrap().state = 104;
                        send.send(Msg::AutoSave)?;
                        state.lock().unwrap().state = 105;
                        send.send(Msg::WalkFinished(rproc.path.clone()))?;
                        proc = None;
                    }
                }
            }
        }

        Ok(())
    }

    fn load_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        let mut last_count = 0;

        loop {
            match recv.recv()? {
                Msg::Quit => {
                    state.lock().unwrap().state = 1;
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    state.lock().unwrap().state = 2;
                    print_(printer, format!("loading {}", last_count));
                    send.send(Msg::Debug)?;
                }
                Msg::Load(count, filter, absolute, relative) => {
                    state.lock().unwrap().state = 3;
                    last_count = count;
                    let (filter, txt) = load_file(filter, &absolute)?;
                    if filter == FileFilter::Binary {
                        if let Ok(mut log) = data.log.try_clone() {
                            let _ = writeln!(log, "maybe binary file {}", relative);
                        }
                    } else if filter != FileFilter::Ignore {
                        send.send(Msg::Index(count, filter, absolute, relative, txt))?;
                    }
                }
                msg => {
                    state.lock().unwrap().state = 4;
                    send.send(msg)?;
                }
            }
        }
        Ok(())
    }

    fn index_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        let mut last_count = 0;

        loop {
            match recv.recv()? {
                Msg::Quit => {
                    state.lock().unwrap().state = 1;
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    state.lock().unwrap().state = 2;
                    print_(printer, format!("indexing {}", last_count));
                    send.send(Msg::Debug)?;
                }
                Msg::Index(count, filter, _absolute, relative, txt) => {
                    let Ok(mut log) = data.log.try_clone() else {
                        panic!();
                    };

                    state.lock().unwrap().state = 3;
                    last_count = count;
                    let (filter, words) = indexing(&mut log, filter, &relative, &txt)?;
                    match filter {
                        FileFilter::Binary => {
                            let _ = writeln!(log, "binary file {}", relative);
                            // send.send(Msg::MergeWords(count, words))?;
                        }
                        FileFilter::Text | FileFilter::Html => {
                            send.send(Msg::MergeWords(count, words))?;
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                }
                msg => {
                    state.lock().unwrap().state = 4;
                    send.send(msg)?;
                }
            }
        }
        Ok(())
    }

    fn merge_words_proc(
        recv: Receiver<Msg>,
        send: Sender<Msg>,
        state: Arc<Mutex<WorkerState>>,
        data: &'static Data,
        printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    ) -> Result<(), AppError> {
        let mut last_count = 0;

        loop {
            match recv.recv()? {
                Msg::Quit => {
                    state.lock().unwrap().state = 1;
                    send.send(Msg::Quit)?;
                    break;
                }
                Msg::Debug => {
                    state.lock().unwrap().state = 2;
                    print_(printer, format!("merge words {}", last_count));
                    send.send(Msg::Debug)?;
                }
                Msg::MergeWords(count, words) => {
                    state.lock().unwrap().state = 3;
                    last_count = count;
                    print_err_(
                        printer,
                        data.log.try_clone().unwrap(),
                        "merge_words",
                        merge_words(data, &state, words, printer),
                    );
                }
                msg => {
                    state.lock().unwrap().state = 4;
                    send.send(msg)?;
                }
            }
        }
        Ok(())
    }
}

pub fn shut_down(work: &Work) {
    println!("sending shutdown!");
    if let Err(e) = work.send.send(Msg::Quit) {
        if let Ok(mut print) = work.printer.lock() {
            let _ = print.print(format!("shutdown {:?}", e));
        }
    }

    if let Ok(mut print) = work.printer.lock() {
        let _ = print.print("wait on shutdown".into());
    }

    sleep(Duration::from_millis(100));

    for w in work.workers.iter() {
        if !w.handle.is_finished() {
            continue;
        }
    }
}

pub fn load_file(filter: FileFilter, absolute: &Path) -> Result<(FileFilter, Vec<u8>), AppError> {
    let mut buf = Vec::new();
    File::open(&absolute)?.read_to_end(&mut buf)?;

    let filter = if filter == FileFilter::Inspect {
        content_filter(&buf)
    } else {
        filter
    };

    Ok((filter, buf))
}

pub fn indexing(
    log: &mut File,
    filter: FileFilter,
    relative: &str,
    txt: &Vec<u8>,
) -> Result<(FileFilter, TmpWords), io::Error> {
    let mut words = TmpWords::new(relative);
    let txt = String::from_utf8_lossy(txt.as_ref());

    match filter {
        FileFilter::Text => {
            index_txt2(log, relative, &mut words, txt.as_ref())?;
        }
        FileFilter::Html => {
            index_html(log, relative, &mut words, txt.as_ref())?;
        }
        FileFilter::Ignore => {}
        FileFilter::Inspect => {}
        FileFilter::Binary => {}
    }

    Ok((filter, words))
}

fn merge_words(
    data: &'static Data,
    state: &Arc<Mutex<WorkerState>>,
    words_buffer: TmpWords,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    let do_auto_save = {
        state.lock().unwrap().state = 100;
        let mut write = data.words.lock()?;
        state.lock().unwrap().state = 101;
        timing(printer, "merge", 100, || write.append(words_buffer))?;
        state.lock().unwrap().state = 102;

        let auto_save = write.should_auto_save();
        if auto_save {
            let last = write.save_time();
            print_(printer, format!("loop-time {:?}", last.elapsed()));
            write.set_save_time();
        }
        auto_save
    };

    if do_auto_save {
        state.lock().unwrap().state = 200;
        timing(printer, "autosave", 1, || auto_save(printer, data))?;
    }

    Ok(())
}

fn terminal_proc(
    recv: &Receiver<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> Result<(), AppError> {
    loop {
        match recv.recv()? {
            Msg::Quit => {
                state.lock().unwrap().state = 1;
                break;
            }
            Msg::Debug => {
                state.lock().unwrap().state = 2;
                print_(printer, "terminal");
            }
            Msg::AutoSave => {
                state.lock().unwrap().state = 3;
                print_err_(
                    printer,
                    data.log.try_clone().unwrap(),
                    "auto_save",
                    auto_save(printer, data),
                );
            }
            Msg::DeleteFile(file) => {
                state.lock().unwrap().state = 4;
                print_err_(
                    printer,
                    data.log.try_clone().unwrap(),
                    "delete_file",
                    delete_file(printer, data, file),
                );
            }
            Msg::WalkFinished(file) => {
                state.lock().unwrap().state = 5;

                print_(printer, "*** final store ***");

                let mut words = data.words.lock()?;
                words.write()?;
                words.compact_blocks();

                print_(printer, format!("*** {:?} finished ***", file));
            }
            msg => {
                state.lock().unwrap().state = 6;
                print_(printer, format!("invalid terminal message {:?}", msg));
            }
        }
    }
    Ok(())
}

pub fn name_filter(path: &Path) -> FileFilter {
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

    const EXT_IGNORE: &[&str] = &[
        "jpg", "pdf", "gif", "css", "png", "doc", "rtf", "js", "ico", "woff", "zip", "jpeg", "odt",
        "docx", "lit", "xml", "epub", "mobi", "exe", "mp3", "azw3", "bmp", "bak", "ccs", "css",
        "dwt", "eot", "img", "pdb", "prc", "psc", "swf", "svg", "wmf", "wpd", "wav", "mso", "mid",
        "thmx", "zblorb", "rm", "ttf", "woff2", "eot", "emz", "mht",
    ];
    const NAME_IGNORE: &[&str] = &[
        ".message.ftp.txt",
        "history.txt",
        ".stored",
        "log.txt",
        "thumbs.db",
    ];

    if EXT_IGNORE.contains(&ext.as_str()) || NAME_IGNORE.contains(&name.as_str()) {
        FileFilter::Ignore
    } else {
        FileFilter::Inspect
    }
}

pub fn content_filter(txt: &Vec<u8>) -> FileFilter {
    const HTML_RECOGNIZE: &[&[u8]] = &[
        b"<!--ADULTSONLY",
        b"<--",
        b"<head",
        b"<HTML",
        b"<html",
        b"<?xml",
        b"<!DOCTYPE",
        b"<!doctype",
        b"_<!DOCTYPE",
    ];

    // omit starting whitespace
    let mut start_idx = 0;
    for i in 0..256 {
        if txt[i] != b' ' && txt[i] != b'\t' {
            start_idx = i;
            break;
        }
    }
    // dont scan everything
    let txt_part = &txt[start_idx..min(start_idx + 256, txt.len())];

    if HTML_RECOGNIZE.iter().any(|v| txt_part.starts_with(*v)) {
        FileFilter::Html
    } else {
        for c in txt_part.iter().copied() {
            #[allow(unused_comparisons)]
            if c >= 0 && c <= 8 || c >= 11 && c <= 12 || c >= 14 && c <= 31 {
                return FileFilter::Binary;
            }
        }
        FileFilter::Text
    }
}

pub fn auto_save(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
) -> Result<(), AppError> {
    data.write()?;
    Ok(())
}

fn delete_file(
    _printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    data: &'static Data,
    file: String,
) -> Result<(), AppError> {
    let mut write = data.words.lock()?;
    write.remove_file(file);

    Ok(())
}

fn print_<S: Into<String>>(printer: &Arc<Mutex<dyn ExternalPrinter + Send>>, msg: S) {
    if let Ok(mut print) = printer.lock() {
        let _ = print.print(msg.into());
    }
}

fn print_err_(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    mut log: File,
    task: &str,
    res: Result<(), AppError>,
) {
    if let Err(err) = res {
        let _ = writeln!(log, "{} {:#?}", task, err);
        if let Ok(mut print) = printer.lock() {
            let _ = print.print(format!("{} {:?}", task, err));
        }
    }
}

pub fn timing<S: AsRef<str>, R>(
    printer: &Arc<Mutex<dyn ExternalPrinter + Send>>,
    name: S,
    threshold: u64,
    fun: impl FnOnce() -> R,
) -> R {
    let now = Instant::now();

    let result = fun();

    let timing = now.elapsed();
    if timing > Duration::from_millis(threshold) {
        print_(printer, format!("{} {:?}", name.as_ref(), now.elapsed()));
    }

    result
}

// Search the result files and return matching text-lines.
pub fn find_matched_lines(
    terms: &[String],
    files: &Vec<String>,
) -> Result<Vec<(String, Vec<String>)>, AppError> {
    let terms: Vec<_> = terms.iter().map(|v| WildMatch::new(v)).collect();

    // get the text-lines that contain any of the search-terms.
    let mut result = Vec::new();
    for file in files {
        let path = PathBuf::from(".");
        let path = path.join(&file);

        let (_filter, txt) = load_file(FileFilter::Inspect, &path)?;
        let txt = String::from_utf8_lossy(txt.as_ref());
        let mut text_lines = Vec::new();
        for line in txt.split('\n') {
            let mut print_line = false;

            'line: for word in line.split(' ') {
                for term in &terms {
                    if term.matches(word) {
                        print_line = true;
                        break 'line;
                    }
                }
            }

            if print_line {
                text_lines.push(line.to_string());
            }
        }

        result.push((file.clone(), text_lines));
    }

    Ok(result)
}

pub mod parse {
    use kparse::combinators::{pchar, track};
    use kparse::KParseError;
    use kparse::{
        define_span, Code, ErrInto, KParser, ParseSpan, TokenizerError, Track, TrackResult,
    };
    use nom::branch::alt;
    use nom::bytes::complete::{tag, take_while, take_while1};
    use nom::character::complete::one_of;
    use nom::combinator::recognize;
    use nom::sequence::{preceded, terminated, tuple};
    use nom::{InputIter, InputTake, Slice};
    use std::fmt::{Debug, Display, Formatter};

    #[derive(Debug, PartialEq, Clone, Copy, Eq)]
    pub enum TxtCode {
        NomError,

        Text,

        Word,
        Pgp,
        Base64,
        KeyValue,

        WordTok,
        NonWord,
        Base64Begin,
        Base64Line,
        Base64Stop,
        Base64End,
        PgpStart,
        PgpEnd,
        PgpSpecial,
        Key,
        Any,
        AtNewline,
        NewLine,
        WhiteSpace,
        Eof,
    }

    impl Display for TxtCode {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    impl Code for TxtCode {
        const NOM_ERROR: Self = Self::NomError;
    }

    #[derive(Debug)]
    pub enum TxtPart<'s> {
        Pgp(Span<'s>),
        Base64,
        KeyValue(Span<'s>),
        Text(Span<'s>),
        NonText,
        NewLine,
        Eof,
    }

    define_span!(Span = TxtCode, str);
    pub type ParserResult<'s, O> = kparse::ParserResult<TxtCode, Span<'s>, O>;
    pub type TokenizerResult<'s> = kparse::TokenizerResult<TxtCode, Span<'s>, Span<'s>>;
    pub type NomResult<'s> = kparse::ParserResult<TxtCode, Span<'s>, Span<'s>>;
    pub type ParserError<'s> = kparse::ParserError<TxtCode, Span<'s>>;

    pub fn parse_txt(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        track(
            TxtCode::Text,
            alt((
                preceded(newline.err_into(), parse_pgp),
                preceded(newline.err_into(), parse_base64),
                preceded(newline.err_into(), parse_key_value),
                parse_word,
                parse_nonword,
                parse_newline,
                parse_eof,
            )),
        )(input)
        .with_code(TxtCode::Text)
    }

    pub fn parse_eof(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        Track.enter(TxtCode::Eof, input);
        if input.len() == 0 {
            Track.ok(input, input, TxtPart::Eof)
        } else {
            Track.err(ParserError::new(TxtCode::Eof, input))
        }
    }

    pub fn parse_newline(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        let (rest, _v) = track(TxtCode::NewLine, newline)(input)
            .with_code(TxtCode::NewLine)
            .err_into()?;
        Ok((rest, TxtPart::NewLine))
    }

    pub fn parse_word(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        let (rest, v) = track(TxtCode::Word, terminated(tok_word, tok_non_word0))(input)
            .with_code(TxtCode::Word)
            .err_into()?;
        Ok((rest, TxtPart::Text(v)))
    }

    pub fn parse_nonword(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        let (rest, _v) = track(TxtCode::NonWord, tok_non_word1)(input)
            .with_code(TxtCode::Word)
            .err_into()?;
        Ok((rest, TxtPart::NonText))
    }

    pub fn tok_word(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::WordTok,
            recognize(take_while1(|c: char| c.is_alphabetic())),
        )(input)
        .with_code(TxtCode::Word)
    }

    pub fn tok_non_word1(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::NonWord,
            recognize(take_while1(|c: char| !c.is_alphabetic() && c != '\n')),
        )(input)
        .with_code(TxtCode::NonWord)
    }

    pub fn tok_non_word0(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::NonWord,
            recognize(take_while(|c: char| !c.is_alphabetic() && c != '\n')),
        )(input)
        .with_code(TxtCode::NonWord)
    }

    pub fn parse_pgp(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        Track.enter(TxtCode::Pgp, input);
        let (rest, v) = recognize(tuple((
            whitespace,
            tag("-----BEGIN PGP SIGNATURE-----"),
            tok_any_until_new_line,
            newline,
            //
            tok_pgp_text,
            //
            tag("END PGP SIGNATURE-----"),
            tok_any_until_new_line,
        )))(input)
        .with_code(TxtCode::Pgp)
        .err_into()
        .track()?;
        Track.ok(rest, input, TxtPart::Pgp(v))
    }

    pub fn tok_pgp_text(input: Span<'_>) -> TokenizerResult<'_> {
        Track.enter(TxtCode::PgpSpecial, input);

        let mut it = input.iter_indices();
        'l: loop {
            match it.next() {
                Some((pos, '-')) => {
                    for _ in 0..4 {
                        if let Some((_pos, c)) = it.next() {
                            if c != '-' {
                                continue 'l;
                            }
                        } else {
                            continue 'l;
                        }
                    }
                    return Track.ok(input.slice(pos + 5..), input, input.slice(..pos + 5));
                }
                Some((_, _)) => {}
                None => {
                    return Track.err(TokenizerError::new(TxtCode::PgpSpecial, input));
                }
            }
        }
    }

    pub fn parse_base64(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        let rest = input;

        let (rest, _v) = tok_base64_begin(rest).err_into()?;

        let mut rest2 = rest;
        loop {
            let (rest3, v) = alt((
                preceded(newline, tok_base64_stop),
                preceded(newline, tok_base64_line),
            ))(rest2)
            .err_into()?;

            rest2 = rest3;

            if *v.fragment() == "`" {
                break;
            }
        }
        let rest = rest2;

        let (rest, _v) = tok_base64_end(rest).err_into()?;

        Ok((rest, TxtPart::Base64))
    }

    pub fn tok_base64_begin(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::Base64Begin,
            recognize(tuple((whitespace, tag("begin"), tok_any_until_new_line))),
        )(input)
        .with_code(TxtCode::Base64Begin)
    }

    pub fn tok_base64_line(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::Base64Line,
            recognize(tuple((whitespace, tok_any_until_new_line1))),
        )(input)
        .with_code(TxtCode::Base64Line)
    }

    pub fn tok_base64_stop(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::Base64Stop,
            recognize(tuple((pchar('`'), tok_at_new_line))),
        )(input)
        .with_code(TxtCode::Base64Stop)
    }

    pub fn tok_base64_end(input: Span<'_>) -> TokenizerResult<'_> {
        track(
            TxtCode::Base64End,
            recognize(tuple((whitespace, tag("end"), tok_any_until_new_line))),
        )(input)
        .with_code(TxtCode::Base64End)
    }

    pub fn parse_key_value(input: Span<'_>) -> ParserResult<'_, TxtPart> {
        let (rest, v) = track(
            TxtCode::KeyValue,
            recognize(tuple((tok_key, pchar(':'), tok_any_until_new_line))),
        )(input)
        .with_code(TxtCode::KeyValue)
        .err_into()?;

        Ok((rest, TxtPart::KeyValue(v)))
    }

    pub fn tok_key(input: Span<'_>) -> TokenizerResult<'_> {
        Track.enter(TxtCode::Key, input);
        let (rest, v) =
            take_while(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '*')(input)
                .with_code(TxtCode::Key)
                .track()?;
        Track.ok(rest, input, v)
    }

    pub fn tok_any_until_new_line1(input: Span<'_>) -> TokenizerResult<'_> {
        Track.enter(TxtCode::Any, input);
        let (rest, v) = take_while1(|c: char| c != '\n')(input)
            .with_code(TxtCode::Any)
            .track()?;
        Track.ok(rest, input, v)
    }

    pub fn tok_any_until_new_line(input: Span<'_>) -> TokenizerResult<'_> {
        Track.enter(TxtCode::Any, input);
        let (rest, v) = take_while(|c: char| c != '\n')(input)
            .with_code(TxtCode::Any)
            .track()?;
        Track.ok(rest, input, v)
    }

    pub fn tok_at_new_line(input: Span<'_>) -> TokenizerResult<'_> {
        Track.enter(TxtCode::AtNewline, input);
        match input.iter_elements().next() {
            Some('\n') => Track.ok(input, input, input.take(0)),
            _ => Track.err(TokenizerError::new(TxtCode::AtNewline, input)),
        }
    }

    pub fn newline(input: Span<'_>) -> TokenizerResult<'_> {
        recognize(one_of("\n\r"))(input).with_code(TxtCode::NewLine)
    }

    pub fn whitespace(input: Span<'_>) -> TokenizerResult<'_> {
        take_while(|c: char| c == ' ' || c == '\t' || c.is_whitespace())(input)
            .with_code(TxtCode::WhiteSpace)
    }
}

pub fn index_txt2(
    log: &mut File,
    relative: &str,
    tmp_words: &mut TmpWords,
    text: &str,
) -> Result<usize, io::Error> {
    let mut n_words = 0usize;

    let tracker = Track::new_tracker::<TxtCode, _>();
    let mut input = Track::new_span(&tracker, text);
    'l: loop {
        match parse::parse_txt(input) {
            Ok((rest, v)) => {
                // dbg!(&v);
                match v {
                    TxtPart::Text(v) => {
                        n_words += 1;
                        let word = v.to_lowercase();
                        if STOP_WORDS
                            .binary_search_by(|probe| (*probe).cmp(word.as_ref()))
                            .is_ok()
                        {
                            continue 'l;
                        }
                        // spurios tags
                        if word.contains('<') || word.contains(">") {
                            continue 'l;
                        }
                        tmp_words.add_word(word);
                    }
                    TxtPart::Eof => {
                        break 'l;
                    }
                    TxtPart::Pgp(_) => {}
                    TxtPart::Base64 => {}
                    TxtPart::KeyValue(_) => {}
                    TxtPart::NonText => {}
                    TxtPart::NewLine => {}
                }
                // let r = tracker.results();
                // writeln!(log, "{:#?}", r)?;

                input = rest;
            }
            Err(e) => {
                let r = tracker.results();
                println!("{}", relative);
                println!("{:#?}", e);
                println!("{:#?}", r);
                writeln!(log, "{}", relative)?;
                writeln!(log, "{:#?}", e)?;
                writeln!(log, "{:#?}", r)?;
            }
        }
    }

    Ok(n_words)
}

pub fn index_html(
    log: &mut File,
    relative: &str,
    words: &mut TmpWords,
    buf: &str,
) -> Result<(), io::Error> {
    #[derive(Debug)]
    struct IdxSink {
        pub txt: String,
        pub elem: Vec<QualName>,
        // pub comment: Vec<StrTendril>,
        // pub pi: Vec<(StrTendril, StrTendril)>,
    }

    #[derive(Clone, Debug)]
    enum IdxHandle {
        Elem(usize),
        Comment(usize),
        Pi(usize),
    }

    impl TreeSink for &mut IdxSink {
        type Handle = IdxHandle;
        type Output = ();

        fn finish(self) -> Self::Output {}

        fn parse_error(&mut self, _msg: Cow<'static, str>) {
            // println!("parse_error {:?} {:?}", _msg, self);
        }

        fn get_document(&mut self) -> Self::Handle {
            IdxHandle::Elem(0)
        }

        fn elem_name<'c>(&'c self, target: &'c Self::Handle) -> ExpandedName<'c> {
            match target {
                IdxHandle::Elem(i) => self.elem[*i].expanded(),
                IdxHandle::Comment(_) => unimplemented!(),
                IdxHandle::Pi(_) => unimplemented!(),
            }
        }

        fn create_element(
            &mut self,
            name: QualName,
            _attrs: Vec<Attribute>,
            _flags: ElementFlags,
        ) -> Self::Handle {
            let handle = self.elem.len();
            self.elem.push(name);
            IdxHandle::Elem(handle)
        }

        fn create_comment(&mut self, _text: StrTendril) -> Self::Handle {
            // no need to store, always hand out 0
            // let handle = self.comment.len();
            // self.comment.push(text);
            IdxHandle::Comment(0)
        }

        fn create_pi(&mut self, _target: StrTendril, _data: StrTendril) -> Self::Handle {
            // no need to store, always hand out 0
            // let handle = self.pi.len();
            // self.pi.push((target, data));
            IdxHandle::Pi(0)
        }

        fn append(&mut self, _parent: &Self::Handle, child: NodeOrText<Self::Handle>) {
            match child {
                NodeOrText::AppendNode(_n) => {}
                NodeOrText::AppendText(v) => {
                    self.txt.push_str(v.as_ref());
                }
            }
        }

        fn append_based_on_parent_node(
            &mut self,
            _element: &Self::Handle,
            _prev_element: &Self::Handle,
            _child: NodeOrText<Self::Handle>,
        ) {
        }

        fn append_doctype_to_document(
            &mut self,
            _name: StrTendril,
            _public_id: StrTendril,
            _system_id: StrTendril,
        ) {
        }

        fn get_template_contents(&mut self, target: &Self::Handle) -> Self::Handle {
            target.clone()
        }

        fn same_node(&self, _x: &Self::Handle, _y: &Self::Handle) -> bool {
            false
        }

        fn set_quirks_mode(&mut self, _mode: QuirksMode) {}

        fn append_before_sibling(
            &mut self,
            _sibling: &Self::Handle,
            _new_node: NodeOrText<Self::Handle>,
        ) {
        }

        fn add_attrs_if_missing(&mut self, _target: &Self::Handle, _attrs: Vec<Attribute>) {}

        fn remove_from_parent(&mut self, _target: &Self::Handle) {}

        fn reparent_children(&mut self, _node: &Self::Handle, _new_parent: &Self::Handle) {}
    }

    let mut s = IdxSink {
        txt: String::default(),
        elem: Vec::default(),
    };

    let p = parse_document(&mut s, ParseOpts::default());
    p.one(buf);

    index_txt2(log, relative, words, s.txt.as_str())?;

    Ok(())
}
