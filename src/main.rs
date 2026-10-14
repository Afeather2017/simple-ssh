use tokio::net::{ TcpListener, TcpStream };
use tokio::net::tcp::{ ReadHalf, WriteHalf };
use tokio::io::{ self, AsyncReadExt, AsyncBufReadExt, BufReader, AsyncWriteExt };
use std::net::{ SocketAddr };
use tokio::process::Command;
use std::process::Stdio;
use clap::{Parser, ValueEnum};
use strum::Display;
use serde::{Deserialize, Serialize};
use serde_json::{ json, to_vec };
#[derive(Deserialize, Serialize, Debug)]
struct CommandArgs {
    close: Option<bool>,
    args: Vec<String>,
}
async fn run_command(read_half: &mut BufReader<ReadHalf<'_>>, write_half: &mut WriteHalf<'_>, args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    println!("handle command {:?}", args);
    let mut process = match Command::new(&args[0])
        .args(&args[1..])
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn() {
        Ok(p) => p,
        Err(e) => {
            println!("create process failed with {}", e);
            return Ok(());
        }
    };
    let (mut stdout, mut stderr, mut stdin) = match (
        process.stdout.take(),
        process.stderr.take(),
        process.stdin.take(),
    ) {
        (Some(stdout), Some(stderr), Some(stdin)) => (stdout, stderr, stdin),
        _ => return Err("stdout, stderr or stdin take failed".into()),
    };
    loop {
        let mut stream_buf = [0;1024];
        let mut stdout_buf = [0;1024];
        let mut stderr_buf = [0;1024];
        tokio::select! {
            result = read_half.read(&mut stream_buf) => {
                match result {
                    Ok(0) => {
                        println!("stream read 0B");
                        return Ok(());
                    },
                    Ok(n) => {
                        println!("stream read {}B", &n);
                        if let Err(e) = stdin.write(&stream_buf[..n]).await {
                            println!("process write error");
                            return Err(Box::new(e));
                        }
                    },
                    Err(e) => {
                        println!("stream error");
                        return Err(Box::new(e));
                    }
                }
            },
            result = stdout.read(&mut stdout_buf) => {
                match result {
                    Ok(0) => {
                        println!("stdout read 0B");
                        return Ok(());
                    },
                    Ok(n) => {
                        println!("stdout read {}B", &n);
                        if let Err(e) = write_half.write(&stdout_buf[..n]).await {
                            println!("stdout->stream write error");
                            return Err(Box::new(e));
                        }
                    },
                    Err(e) => {
                        println!("process error");
                        return Err(Box::new(e));
                    }
                }
            },
            result = stderr.read(&mut stderr_buf) => {
                match result {
                    Ok(0) => {
                        println!("stderr read 0B");
                        return Ok(());
                    }
                    Ok(n) => {
                        println!("stderr read {}B", &n);
                        if let Err(e) = write_half.write(&stderr_buf[..n]).await {
                            println!("stderr->stream write error");
                            return Err(Box::new(e));
                        }
                    },
                    Err(e) => {
                        println!("process error");
                        return Err(Box::new(e));
                    }
                }
            },
        }
    }
}

async fn handle_socket(mut stream: TcpStream, remote_addr: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("open for remote {}", remote_addr);
    let (read_half, mut write_half) = stream.split();
    let mut reader = BufReader::new(read_half);

    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        let cmd = serde_json::from_str::<CommandArgs>(&line)?;
        println!("receive command {:?}", cmd);
        if cmd.args.len() == 0 {
            continue;
        }
        if let Err(e) = run_command(&mut reader, &mut write_half, cmd.args).await {
            let _ = write_half.write((e.to_string() + "\n").as_bytes()).await?;
        }
        if cmd.close == Some(true) {
            return Ok(());
        }
    }

}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Display)]
enum Mode {
    Server,
    Client,
}
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 9999)]
    port: u16,

    #[arg(short, long, default_value_t = "0.0.0.0".to_string())]
    addr: String,

    #[arg(short, long, default_value_t = Mode::Client)]
    mode: Mode,

    #[arg(short, long, num_args(1..))]
    command: Vec<String>,
}
async fn run_server(listen_addr: String, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    println!("use {}:{}", listen_addr, port);
    let listener = TcpListener::bind(format!("{}:{}", listen_addr, port)).await?;
    loop {
        let (socket, addr) = listener.accept().await?;
        tokio::task::spawn_local(async move {
            match handle_socket(socket, &addr).await {
                Ok(_) => {
                    println!("connection to {} done", addr);
                },
                Err(e) => {
                    println!("connectoin to {} broken with {}", addr, e);
                }
            };
        });
    }
}
async fn run_client(addr: String, port: u16, command: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(format!("{}:{}", addr, port)).await?;
    let (mut read_half, mut write_half) = stream.split();
    let mut to_send = serde_json::to_vec(&CommandArgs {
        close: Some(true),
        args: command,
    })?;
    to_send.push(b'\n');
    println!("send command");
    write_half.write(&to_send).await?;
    loop {
        let mut stream_buf = [0;1024];
        match read_half.read(&mut stream_buf).await {
            Ok(0) => {
                return Ok(());
            },
            Ok(n) => {
                let _ = io::stdout().write(&stream_buf[..n]).await?;
            },
            Err(e) => {
                return Err(Box::new(e));
            }
        }
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    match args.mode {
        Mode::Server => run_server(args.addr, args.port).await,
        Mode::Client => run_client(args.addr, args.port, args.command).await,
    }
}
