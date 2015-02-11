require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'influxdb'
require 'timeout'
require 'json'

module Sensu
  module Extension
    # Sensu Influx Handler
    class Influx < Handler
      def name
        'influx'
      end

      def description
        'outputs metrics to InfluxDB'
      end

      def post_init
        @influxdb = InfluxDB::Client.new(
          settings['influx']['database'],
          host: settings['influx']['host'],
          port: settings['influx']['port'],
          username: settings['influx']['user'],
          password: settings['influx']['password']
        )

        @timeout = @settings['influx']['timeout'] || 15
      end

      def parse_event(event)
        event = JSON.parse(event)
        [
          event['client']['name'],
          event['check']['name'],
          event['check']['output']
        ]
      rescue => e
        @logger.error "InfluxDB: Error setting up event object - #{e.backtrace}"
      end

      def strip_metric(metric)
        return metric unless @settings['influx']['strip_metric']
        metric.gsub(/^.*#{@settings['influx']['strip_metric']}\.(.*$)/, '\1')
      end

      def parse_value(value)
        return Integer(value) if value.match('\.').nil?
        return Float(value)
      rescue
        value.to_s
      end

      def parse_output_lines(output, host)
        output.split(/\n/).map do |line|
          @logger.debug("Parsing line: #{line}")
          m, v, t = line.split(/\s+/)

          m = strip_metric(m)

          v = parse_value(v)

          points << { time: t.to_f, host: host, metric: m, value: v }
        end
      rescue => e
        @logger.error "InfluxDB: Error parsing output lines - #{e.backtrace}"
        @logger.error "InfluxDB: #{output}"
      end

      def run(event)
        host, series, output = parse_event(event)

        points = parse_output_lines(output, host)

        begin
          @influxdb.write_point(series, points, true)
        rescue => e
          @logger.error("InfluxDB: Error posting event - #{e.backtrace}")
        end

        yield('InfluxDB: Handler finished', 0)
      end
    end
  end
end
