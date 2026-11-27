use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::ExitCode,
};

use catalog::directory::{DIRECTORY_MANIFEST, DirectoryManifest, MAX_MANIFEST_BYTES};
use graph::{
    types::{DAG, Kind},
    validator::validate_assets,
};
use mlua::{Lua, LuaSerdeExt, MultiValue, Value, chunk::ChunkMode};
use serde::Deserialize;
use uuid::Uuid;

mod paths;

#[derive(Deserialize)]
struct JobPin {
    node_run: NodeRunPin,
}

#[derive(Deserialize)]
struct NodeRunPin {
    graph_id: Uuid,
    version: u64,
    node_id: Uuid,
}

struct Mounts {
    job: PathBuf,
    graph: PathBuf,
    input: PathBuf,
    output: PathBuf,
    asset: PathBuf,
}

impl Mounts {
    fn from_env() -> Self {
        Self {
            job: path_from_env("RAYFLOW_JOB_FILE", "/rayflow/job.json"),
            graph: path_from_env("RAYFLOW_GRAPH_FILE", "/rayflow/graph.json"),
            input: path_from_env("RAYFLOW_INPUT_DIR", "/data/inputs"),
            output: path_from_env("RAYFLOW_OUTPUT_DIR", "/data/outputs"),
            asset: path_from_env("RAYFLOW_ASSET_DIR", "/data/assets"),
        }
    }
}

fn path_from_env(name: &str, default: &str) -> PathBuf {
    env::var_os(name).map_or_else(|| PathBuf::from(default), PathBuf::from)
}

fn main() -> ExitCode {
    let mut args = env::args_os();
    let program = args.next().unwrap_or_else(|| "runlua".into());
    let Some(mut script) = args.next() else {
        eprintln!("Usage: runlua <script> [args...]");
        return ExitCode::from(2);
    };
    if script == "-h" || script == "--help" {
        println!("Usage: runlua <script> [args...]");
        return ExitCode::SUCCESS;
    }
    if script == "--" {
        let Some(path) = args.next() else {
            eprintln!("Usage: runlua <script> [args...]");
            return ExitCode::from(2);
        };
        script = path;
    }
    match run(&program, Path::new(&script), args, Mounts::from_env()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("runlua: {error}");
            ExitCode::FAILURE
        }
    }
}

fn read(path: &Path) -> mlua::Result<Vec<u8>> {
    fs::read(path).map_err(|error| mlua::Error::runtime(format!("{}: {error}", path.display())))
}

fn metadata(path: &Path) -> mlua::Result<serde_json::Value> {
    serde_json::from_slice(&read(path)?)
        .map_err(|error| mlua::Error::runtime(format!("{}: {error}", path.display())))
}

fn required_file(root: &Path, name: &str) -> mlua::Result<PathBuf> {
    let path = paths::existing(root, name)?
        .ok_or_else(|| mlua::Error::runtime(format!("file is missing: {name}")))?;
    if !path.is_file() {
        return Err(mlua::Error::runtime(format!(
            "expected a regular file: {name}"
        )));
    }
    Ok(path)
}

fn run(
    program: &OsStr,
    script: &Path,
    arguments: impl IntoIterator<Item = OsString>,
    mounts: Mounts,
) -> mlua::Result<()> {
    let script_bytes = read(script)?;
    let job = metadata(&mounts.job)?;
    let graph = metadata(&mounts.graph)?;
    let pin = JobPin::deserialize(&job)
        .map_err(mlua::Error::external)?
        .node_run;
    let dag = DAG::deserialize(&graph).map_err(mlua::Error::external)?;
    if dag.id != pin.graph_id || dag.version != pin.version {
        return Err(mlua::Error::runtime(
            "graph does not match the pinned node run",
        ));
    }
    let (index, node) = dag
        .nodes
        .into_iter()
        .enumerate()
        .find(|(_, node)| node.id == pin.node_id)
        .ok_or_else(|| mlua::Error::runtime("job node is missing from the pinned graph"))?;
    validate_assets(&node).map_err(mlua::Error::external)?;

    let lua = Lua::new();
    let rayflow = lua.create_table()?;
    rayflow.set("job", lua.to_value(&job)?)?;
    rayflow.set("graph", lua.to_value(&graph)?)?;
    rayflow.set("node", lua.to_value(&graph["nodes"][index])?)?;

    let input = mounts.input;
    let input_for_list = input.clone();
    let inputs = node.inputs;
    let list_inputs = inputs.clone();
    let outputs = node.outputs;
    let assets = node.assets;
    rayflow.set(
        "input_path",
        lua.create_function(move |lua, (name, member): (String, Option<String>)| {
            let port = inputs
                .iter()
                .find(|port| port.file_name == name)
                .ok_or_else(|| mlua::Error::runtime(format!("input is not declared: {name}")))?;
            let relative = paths::relative(&name, member.as_deref())?;
            let Some(path) = paths::existing(&input, &name)? else {
                return if port.required {
                    Err(mlua::Error::runtime(format!(
                        "required input is missing: {name}"
                    )))
                } else {
                    Ok(None)
                };
            };
            let path = if member.is_some() {
                if !path.is_dir() {
                    return Err(mlua::Error::runtime(format!(
                        "input is not a directory: {name}"
                    )));
                }
                paths::existing(&input, &relative)?.ok_or_else(|| {
                    mlua::Error::runtime(format!("input member is missing: {relative}"))
                })?
            } else {
                path
            };
            lua.create_string(path.as_os_str().as_encoded_bytes())
                .map(Some)
        })?,
    )?;

    rayflow.set(
        "list_input",
        lua.create_function(move |lua, name: String| {
            let port = list_inputs
                .iter()
                .find(|port| port.file_name == name)
                .ok_or_else(|| mlua::Error::runtime(format!("input is not declared: {name}")))?;
            let name = paths::relative(&name, None)?;
            let Some(path) = paths::existing(&input_for_list, &name)? else {
                return if port.required {
                    Err(mlua::Error::runtime(format!(
                        "required input is missing: {name}"
                    )))
                } else {
                    Ok(Value::Nil)
                };
            };
            if !path.is_dir() {
                return Err(mlua::Error::runtime(format!(
                    "input is not a directory: {name}"
                )));
            }
            let manifest = required_file(&path, DIRECTORY_MANIFEST)?;
            let file = fs::File::open(manifest).map_err(mlua::Error::external)?;
            let mut bytes = Vec::new();
            file.take(MAX_MANIFEST_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(mlua::Error::external)?;
            if bytes.len() as u64 > MAX_MANIFEST_BYTES {
                return Err(mlua::Error::runtime("directory manifest is too large"));
            }
            let manifest: DirectoryManifest =
                serde_json::from_slice(&bytes).map_err(mlua::Error::external)?;
            manifest.validate().map_err(mlua::Error::external)?;
            lua.to_value(&manifest.entries)
        })?,
    )?;
    rayflow.set(
        "output_path",
        lua.create_function(move |lua, (name, member): (String, Option<String>)| {
            let port = outputs
                .iter()
                .find(|port| port.file_name == name)
                .ok_or_else(|| mlua::Error::runtime(format!("output is not declared: {name}")))?;
            if port.kind == Kind::File && member.is_some() {
                return Err(mlua::Error::runtime(format!(
                    "output is not a directory: {name}"
                )));
            }
            let relative = paths::relative(&name, member.as_deref())?;
            let directory = port.kind == Kind::Directory && member.is_none();
            let path = paths::output(&mounts.output, &relative, directory)?;
            lua.create_string(path.as_os_str().as_encoded_bytes())
        })?,
    )?;
    rayflow.set(
        "asset_path",
        lua.create_function(move |lua, name: String| {
            if !assets.iter().any(|asset| asset.file_name == name) {
                return Err(mlua::Error::runtime(format!(
                    "asset is not declared: {name}"
                )));
            }
            let path = required_file(&mounts.asset, &name)?;
            lua.create_string(path.as_os_str().as_encoded_bytes())
        })?,
    )?;
    lua.globals().set("rayflow", rayflow)?;

    let args = lua.create_table()?;
    args.set(-1, lua.create_string(program.as_encoded_bytes())?)?;
    args.set(0, lua.create_string(script.as_os_str().as_encoded_bytes())?)?;
    let mut chunk_args = MultiValue::new();
    for (index, arg) in arguments.into_iter().enumerate() {
        let value = lua.create_string(arg.as_encoded_bytes())?;
        args.set(index + 1, value.clone())?;
        chunk_args.push_back(Value::String(value));
    }
    lua.globals().set("arg", args)?;
    lua.load(&script_bytes)
        .set_name(format!("@{}", script.display()))
        .set_mode(ChunkMode::Text)
        .call::<()>(chunk_args)
}
