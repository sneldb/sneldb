pub use super::factories::{
    CandidateZoneFactory, CommandFactory, CompareOpFactory, EventFactory, ExprFactory,
    QueryPlanFactory, ResolverFactory, SchemaRegistryFactory, ZoneCursorFactory, ZoneIndexFactory,
    ZoneMetaFactory, ZonePlanFactory, ZoneRowFactory, ZoneValueLoaderFactory,
};

pub struct Factory;

impl Factory {
    pub fn zone_plan() -> ZonePlanFactory {
        ZonePlanFactory::new()
    }

    pub fn event() -> EventFactory {
        EventFactory::new()
    }

    pub fn candidate_zone() -> CandidateZoneFactory {
        CandidateZoneFactory::new()
    }

    pub fn zone_meta() -> ZoneMetaFactory {
        ZoneMetaFactory::new()
    }

    pub fn zone_index() -> ZoneIndexFactory {
        ZoneIndexFactory::new()
    }

    pub fn zone_cursor() -> ZoneCursorFactory {
        ZoneCursorFactory::new()
    }

    pub fn zone_row() -> ZoneRowFactory {
        ZoneRowFactory::new()
    }

    pub fn zone_value_loader() -> ZoneValueLoaderFactory {
        ZoneValueLoaderFactory::new()
    }

    pub fn store_command() -> CommandFactory {
        CommandFactory::store()
    }

    pub fn define_command() -> CommandFactory {
        CommandFactory::define()
    }

    pub fn query_command() -> CommandFactory {
        CommandFactory::query()
    }

    pub fn replay_command() -> CommandFactory {
        CommandFactory::replay()
    }

    pub fn compare_op() -> CompareOpFactory {
        CompareOpFactory::new()
    }

    pub fn expr() -> ExprFactory {
        ExprFactory::new()
    }

    pub fn schema_registry() -> SchemaRegistryFactory {
        SchemaRegistryFactory::new()
    }

    pub fn query_plan() -> QueryPlanFactory {
        QueryPlanFactory::new()
    }

    pub fn resolver() -> ResolverFactory {
        ResolverFactory::new()
    }
}
