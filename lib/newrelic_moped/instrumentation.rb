require 'new_relic/agent/method_tracer'

DependencyDetection.defer do
  @name = :moped

  depends_on do
    defined?(::Moped) and not NewRelic::Control.instance['disable_moped']
  end

  executes do
    NewRelic::Agent.logger.debug 'Installing Moped instrumentation'
  end

  executes do
    Moped::Node.class_eval do
      include NewRelic::Moped::Instrumentation
      alias_method :logging_without_newrelic_trace, :logging
      alias_method :logging, :logging_with_newrelic_trace
    end
  end

  executes do
     NewRelic::Agent.set_sql_obfuscator(:replace) do |sql|
       NewRelic::Moped::Instrumentation.obsfucate(MultiJson.load(sql))
     end
  end
end

module NewRelic
  module Moped
    module Instrumentation
      def logging_with_newrelic_trace(operations, &blk)
        operation_name, collection, sql = get_tracing_data(operations.first)

        self.class.trace_execution_scoped(["Database/", collection, '/', operation_name].join) do
          t0 = Time.now
          res = logging_without_newrelic_trace(operations, &blk)
          elapsed_time = (Time.now - t0).to_f
          if sql
            NewRelic::Agent.instance.transaction_sampler.notice_sql(sql, nil, elapsed_time)
            NewRelic::Agent.instance.sql_sampler.notice_sql(sql, nil, nil, elapsed_time)
          end
          res
        end
      end

      def get_tracing_data operation
        name = operation.class.name.split('::').last
        collection = :unknown
        cmd = {}

        case operation
        when ::Moped::Protocol::Command,
          ::Moped::Protocol::Query,
          ::Moped::Protocol::Delete

          collection = operation.full_collection_name
          cmd[:selector] = operation.selector

        when ::Moped::Protocol::Update
          collection = operation.full_collection_name
          cmd[:selector] = operation.selector
          cmd[:update] = operation.update

        when ::Moped::Protocol::Insert,
          ::Moped::Protocol::GetMore

          collection = operation.full_collection_name

        when ::Moped::Protocol::KillCursors
        end

        [name, collection, MultiJson.dump(cmd)]
      end

      def self.obsfucate obj
        case obj
        when Hash
          obj.inject({}) do |_h, (k, v)|
            _h[k] = obsfucate(v)
            _h
          end
        when Array
          obj.map {|e| obsfucate(e)}
        else
          '?'
        end
      end
    end
  end
end