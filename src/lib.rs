use anyhow::Error;
use collectd_plugin::{
    collectd_plugin, ConfigItem, Plugin, PluginCapabilities, PluginManager, PluginRegistration,
    Value, ValueList, ValueReport,
};
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

    fn initialize() -> Result<(), Box<dyn error::Error>> {
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
            let providers = self.providers.read().unwrap();
            let provider = providers.find(&path).and_then(Record::get_link);
            if let Some(provider) = provider {
                if provider.is_active() {
                    // TODO: Writed a value
                }
                return Ok(());
            }
        }
        // Creating a new provider
        {
            let mut providers = self.providers.write().unwrap();
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
            Err(err.into())
        } else {
            Ok(())
        }
    }
}

collectd_plugin!(TeleportColelctd);
