use crate::{Env, Expressionista, IrSerializer, QueryGraph, QueryInterpreter, ResponseData};
use schema::QuerySchema;
use tracing::Instrument;

#[derive(Debug)]
pub struct QueryPipeline<'conn> {
    graph: QueryGraph,
    interpreter: QueryInterpreter<'conn>,
    serializer: IrSerializer,
}

impl<'conn> QueryPipeline<'conn> {
    pub fn new(graph: QueryGraph, interpreter: QueryInterpreter<'conn>, serializer: IrSerializer) -> Self {
        Self {
            graph,
            interpreter,
            serializer,
        }
    }

    pub async fn execute(
        mut self,
        query_schema: &QuerySchema,
        trace_id: Option<String>,
    ) -> crate::Result<ResponseData> {
        let serializer = self.serializer;
        let expr = Expressionista::translate(self.graph)?;

        let span = info_span!("prisma:engine:interpret");

        let result = self
            .interpreter
            .interpret(expr, Env::default(), 0, trace_id)
            .instrument(span)
            .await;

        trace!("{}", self.interpreter.log_output());
        serializer.serialize(result?, query_schema)
    }
}
