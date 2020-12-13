use anyhow::Error;
use collectd_plugin::{
    collectd_plugin, CollectdLoggerBuilder, ConfigItem, LogLevel, Plugin, PluginCapabilities,
    PluginManager, PluginManagerCapabilities, PluginRegistration, ValueList, ValueReport,
};
use log::LevelFilter;
use rayon::prelude::*;
use rill::{
    pathfinder::{Pathfinder, Record},
    protocol::Path,
    provider::LogProvider,
    EntryId,
};
use std::collections::HashMap;
use std::error;
use std::sync::RwLock;
use strum::IntoEnumIterator;

struct TeleportColelctd {
    providers: RwLock<Pathfinder<LogProvider>>,
    loggers: RwLock<HashMap<LogLevel, LogProvider>>,
}

impl TeleportColelctd {
    fn new() -> Self {
        let mut loggers = HashMap::new();
        for level in LogLevel::iter() {
            let path = Path::from(vec![EntryId::from("log"), EntryId::from(level.as_ref())]);
            let logger = LogProvider::new(path);
            loggers.insert(level, logger);
        }
        Self {
            providers: RwLock::new(Pathfinder::new()),
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
        rill::install("teleport-collectd")?;
        Ok(())
    }

    fn plugins(
        _config: Option<&[ConfigItem<'_>]>,
    ) -> Result<PluginRegistration, Box<dyn error::Error>> {
        let plugin = Self::new();
        Ok(PluginRegistration::Single(Box::new(plugin)))
    }

    fn shutdown() -> Result<(), Box<dyn error::Error>> {
        rill::terminate()?;
        Ok(())
    }
}

impl TeleportColelctd {
    fn write_value(&self, path: Path, report: &ValueReport) -> Result<(), Error> {
        // Try to find an existent provider
        {
            let providers = self
                .providers
                .read()
                .map_err(|e| Error::msg(e.to_string()))?;
            let provider = providers.find(&path).and_then(Record::get_link);
            if let Some(provider) = provider {
                if provider.is_active() {
                    let value = report.value.to_string();
                    provider.log(value);
                }
                return Ok(());
            }
        }
        // Creating a new provider
        {
            log::info!("Creating a new provider for: {}", path);
            let mut providers = self
                .providers
                .write()
                .map_err(|e| Error::msg(e.to_string()))?;
            let provider = LogProvider::new(path.clone());
            // It can't be active here, since it hadn't existed in the provider.
            providers.dig(path).set_link(provider);
        }
        Ok(())
    }
}

impl Plugin for TeleportColelctd {
    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities::WRITE & PluginCapabilities::LOG
    }

    fn log(&self, lvl: LogLevel, msg: &str) -> Result<(), Box<dyn error::Error>> {
        let loggers = self.loggers.read().map_err(|e| Error::msg(e.to_string()))?;
        // TODO: Replace unwrap to err
        let provider = loggers.get(&lvl).unwrap();
        if provider.is_active() {
            provider.log(msg.to_string());
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
        let err;
        if list.values.len() == 1 {
            let report = list.values.get(0).unwrap();
            err = self.write_value(basic_path, report).err();
        } else {
            err = list.values.par_iter().find_map_last(move |report| {
                let name = EntryId::from(report.name);
                let path = basic_path.concat(&[name]);
                self.write_value(path, report).err()
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
