use collectd_plugin::{
    collectd_plugin, ConfigItem, Plugin, PluginCapabilities, PluginManager, PluginRegistration,
    ValueList,
};
use std::error;

#[derive(Default)]
struct TeleportColelctd {}

impl PluginManager for TeleportColelctd {
    fn name() -> &'static str {
        "teleport-collectd"
    }

    fn plugins(
        _config: Option<&[ConfigItem<'_>]>,
    ) -> Result<PluginRegistration, Box<dyn error::Error>> {
        let plugin = Self {};
        Ok(PluginRegistration::Single(Box::new(plugin)))
    }
}

impl Plugin for TeleportColelctd {
    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities::WRITE
    }

    fn write_values(&self, _list: ValueList<'_>) -> Result<(), Box<dyn error::Error>> {
        Ok(())
    }
}

collectd_plugin!(TeleportColelctd);
