use crate::error::AppError;
use crate::index2::tmp_index::TmpWords;
use crate::proc3::{
    auto_save, delete_file, indexing, load_file, merge_words, name_filter, print_, print_err_,
    Data, FileFilter,
};
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use rustyline::ExternalPrinter;
use std::io::Write;
use std::iter::Flatten;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
#[cfg(feature = "allocator")]
use tracking_allocator::AllocationGroupToken;
use walkdir::WalkDir;

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

pub fn init_work<P: ExternalPrinter + Send + Sync + 'static>(
    printer: P,
    data: &'static Data,
) -> Work {
    #[cfg(feature = "allocator")]
    let mut local_token = AllocationGroupToken::register().expect("token");
    #[cfg(feature = "allocator")]
    println!("init_work gid={}", local_token.id().as_usize().get());
    #[cfg(feature = "allocator")]
    let local_guard = local_token.enter();

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

    #[cfg(feature = "allocator")]
    drop(local_guard);

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
        #[cfg(feature = "allocator")]
        let mut local_token = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        println!("walking gid={}", local_token.id().as_usize().get());
        #[cfg(feature = "allocator")]
        let local_guard = local_token.enter();

        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "walker",
            walk_proc(recv, send, state, data, &printer),
        );

        #[cfg(feature = "allocator")]
        drop(local_guard);
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
                                "new tree walk ignored, still working on the last one.".to_string(),
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
                            send.send(Msg::Load(rproc.count, filter, absolute.into(), relative))?;
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

fn spawn_loading(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        #[cfg(feature = "allocator")]
        let mut local_token = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        println!("loading gid={}", local_token.id().as_usize().get());
        #[cfg(feature = "allocator")]
        let local_guard = local_token.enter();

        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "loading",
            load_proc(recv, send, state, data, &printer),
        );

        #[cfg(feature = "allocator")]
        drop(local_guard);
    })
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
                if filter == FileFilter::Ignore {
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

fn spawn_indexing(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        #[cfg(feature = "allocator")]
        let mut tok_txt = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        let mut tok_html = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        let mut tok_tmpwords = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        println!("indexing txt gid={}", tok_txt.id().as_usize().get());
        #[cfg(feature = "allocator")]
        println!("indexing html gid={}", tok_html.id().as_usize().get());
        #[cfg(feature = "allocator")]
        println!(
            "indexing tmpwords gid={}",
            tok_tmpwords.id().as_usize().get()
        );

        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "indexing",
            index_proc(
                recv,
                send,
                state,
                #[cfg(feature = "allocator")]
                &mut tok_txt,
                #[cfg(feature = "allocator")]
                &mut tok_html,
                #[cfg(feature = "allocator")]
                &mut tok_tmpwords,
                data,
                &printer,
            ),
        );
    })
}

fn index_proc(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    #[cfg(feature = "allocator")] tok_txt: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_html: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_tmpwords: &mut AllocationGroupToken,
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
                let (filter, words) = indexing(
                    &mut log,
                    #[cfg(feature = "allocator")]
                    tok_txt,
                    #[cfg(feature = "allocator")]
                    tok_html,
                    #[cfg(feature = "allocator")]
                    tok_tmpwords,
                    filter,
                    &relative,
                    &txt,
                )?;
                match filter {
                    FileFilter::Ignore => {
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

fn spawn_merge_words(
    recv: Receiver<Msg>,
    send: Sender<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        #[cfg(feature = "allocator")]
        let mut local_token = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        println!("merge_words gid={}", local_token.id().as_usize().get());
        #[cfg(feature = "allocator")]
        let local_guard = local_token.enter();

        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "merge_words",
            merge_words_proc(recv, send, state, data, &printer),
        );

        #[cfg(feature = "allocator")]
        drop(local_guard);
    })
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

fn spawn_terminal(
    recv: Receiver<Msg>,
    state: Arc<Mutex<WorkerState>>,
    data: &'static Data,
    printer: Arc<Mutex<dyn ExternalPrinter + Send>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        #[cfg(feature = "allocator")]
        let mut local_token = AllocationGroupToken::register().expect("token");
        #[cfg(feature = "allocator")]
        println!("terminal gid={}", local_token.id().as_usize().get());
        #[cfg(feature = "allocator")]
        let local_guard = local_token.enter();

        print_err_(
            &printer,
            data.log.try_clone().unwrap(),
            "terminal",
            terminal_proc(&recv, state, data, &printer),
        );

        #[cfg(feature = "allocator")]
        drop(local_guard);
    })
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
