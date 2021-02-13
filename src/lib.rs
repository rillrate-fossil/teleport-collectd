use anyhow::Error;
use collectd_plugin::{
    collectd_plugin, CollectdLoggerBuilder, ConfigItem, LogLevel, Plugin, PluginCapabilities,
    PluginManager, PluginManagerCapabilities, PluginRegistration, ValueList, ValueReport,
};
use log::LevelFilter;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use rillrate::protocol::pathfinder::{Pathfinder, Record};
use rillrate::protocol::provider::{EntryId, Path};
use rillrate::rill::prelude::LogTracer;
use rillrate::RillRate;
use std::collections::HashMap;
use std::error;
use std::sync::{Mutex, RwLock};
use strum::IntoEnumIterator;

static RILLRATE: Lazy<Mutex<Option<RillRate>>> = Lazy::new(|| Mutex::new(None));

struct TeleportColelctd {
    tracers: RwLock<Pathfinder<LogTracer>>,
    loggers: RwLock<HashMap<LogLevel, LogTracer>>,
}

impl TeleportColelctd {
    fn new() -> Self {
        let mut loggers = HashMap::new();
        for level in LogLevel::iter() {
            let path = Path::from(vec![EntryId::from("log"), EntryId::from(level.as_ref())]);
            let logger = LogTracer::new(path, false);
            loggers.insert(level, logger);
        }
        Self {
            tracers: RwLock::new(Pathfinder::new()),
            loggers: RwLock::new(loggers),
        }
    }
}

impl PluginManager for TeleportColelctd {
    fn name() -> &'static str {
        "teleport-collectd"
    }

    fn capabilities() -> PluginManagerCapabilities {
        PluginManagerCapabilities::INIT
    }

    fn initialize() -> Result<(), Box<dyn error::Error>> {
        CollectdLoggerBuilder::new()
            .prefix_plugin::<Self>()
            .filter_level(LevelFilter::Info)
            .try_init()?;
        // TODO: But use `from_config` instead
        // TODO: And prepare that config
        let rillrate = RillRate::from_env("teleport-collectd")?;
        *RILLRATE.lock()? = Some(rillrate);
        Ok(())
    }

    fn plugins(
        _config: Option<&[ConfigItem<'_>]>,
    ) -> Result<PluginRegistration, Box<dyn error::Error>> {
        let plugin = Self::new();
        Ok(PluginRegistration::Single(Box::new(plugin)))
    }

    fn shutdown() -> Result<(), Box<dyn error::Error>> {
        RILLRATE.lock()?.take();
        Ok(())
    }
}

impl TeleportColelctd {
    fn write_value(&self, path: Path, _ts: &str, report: &ValueReport) -> Result<(), Error> {
        // Try to find an existent tracer
        {
            let tracers = self.tracers.read().map_err(|e| Error::msg(e.to_string()))?;
            let tracer = tracers.find(&path).and_then(Record::get_link);
            if let Some(tracer) = tracer {
                if tracer.is_active() {
                    let value = report.value.to_string();
                    // TODO: Convert ts to `SystemTime`
                    tracer.log(value, None);
                }
                return Ok(());
            }
        }
        // Creating a new tracer
        {
            log::info!("Creating a new tracer for: {}", path);
            let mut tracers = self
                .tracers
                .write()
                .map_err(|e| Error::msg(e.to_string()))?;
            let tracer = LogTracer::new(path.clone(), true);
            // It can't be active here, since it hadn't existed in the tracer.
            tracers.dig(path).set_link(tracer);
        }
        Ok(())
    }
}

impl Plugin for TeleportColelctd {
    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities::WRITE | PluginCapabilities::LOG
    }

    fn log(&self, lvl: LogLevel, msg: &str) -> Result<(), Box<dyn error::Error>> {
        let loggers = self.loggers.read().map_err(|e| Error::msg(e.to_string()))?;
        // TODO: Replace unwrap to err
        let tracer = loggers.get(&lvl).unwrap();
        if tracer.is_active() {
            tracer.log(msg.to_string(), None);
        }
        Ok(())
    }

    fn write_values(&self, list: ValueList<'_>) -> Result<(), Box<dyn error::Error>> {
        let host = EntryId::from(list.host);
        let plugin = EntryId::from(list.plugin);
        let plugin_instance = list.plugin_instance.map(EntryId::from);
        let typ = EntryId::from(list.type_);
        let type_instance = list.type_instance.map(EntryId::from);
        let mut entries = Vec::new();
        entries.push(host);
        let skip_type = typ == plugin;
        entries.push(plugin);
        if let Some(value) = plugin_instance {
            entries.push(value);
        }
        if !skip_type {
            entries.push(typ);
        }
        if let Some(value) = type_instance {
            entries.push(value);
        }
        let basic_path = Path::from(entries);
        let ts = list.time.to_string();
        let err;
        if list.values.len() == 1 {
            let report = list.values.get(0).unwrap();
            err = self.write_value(basic_path, &ts, report).err();
        } else {
            err = list.values.par_iter().find_map_last(move |report| {
                let path = basic_path.concat(report.name);
                self.write_value(path, &ts, report).err()
            });
        }
        if let Some(err) = err {
            log::error!("Can't write values: {}", err);
            Err(err.into())
        } else {
            Ok(())
        }
    }
}

collectd_plugin!(TeleportColelctd);
