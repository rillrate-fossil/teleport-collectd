use anyhow::Error;
use collectd_plugin::{
    collectd_plugin, CollectdLoggerBuilder, ConfigItem, Plugin, PluginCapabilities, PluginManager,
    PluginManagerCapabilities, PluginRegistration, ValueList, ValueReport,
};
use log::LevelFilter;
use rayon::prelude::*;
use rill::{
    pathfinder::{Pathfinder, Record},
    protocol::Path,
    provider::LogProvider,
    EntryId,
};
use std::error;
use std::sync::RwLock;

struct TeleportColelctd {
    providers: RwLock<Pathfinder<LogProvider>>,
}

impl Default for TeleportColelctd {
    fn default() -> Self {
        Self {
            providers: RwLock::new(Pathfinder::new()),
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
        let plugin = Self::default();
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
        PluginCapabilities::WRITE
    }

    fn write_values(&self, list: ValueList<'_>) -> Result<(), Box<dyn error::Error>> {
        let host = EntryId::from(list.host);
        let plugin = EntryId::from(list.plugin);
        let err = list.values.par_iter().find_map_last(move |report| {
            let host = host.clone();
            let plugin = plugin.clone();
            let name = EntryId::from(report.name);
            let path = Path::from(vec![host, plugin, name]);
            self.write_value(path, report).err()
        });
        if let Some(err) = err {
            log::error!("Can't write values: {}", err);
            Err(err.into())
        } else {
            Ok(())
        }
    }
}

collectd_plugin!(TeleportColelctd);
