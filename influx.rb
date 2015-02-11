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
          "#{event['check']['name']}.#{event['client']['name']}",
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

      def split_line(line)
        m, v, t = line.split(/\s+/)
        [strip_metric(m), parse_value(v), t]
      end

      def add_point(points, metric, point)
        points[metric] = [] unless points[metric]
        points[metric] << point
        points
      end

      def parse_output_lines(output)
        points = []
        output.split(/\n/).each do |line|
          @logger.debug("Parsing line: #{line}")
          m, v, t = split_line(line)
          points = add_point(points, m, time: t.to_f, value: v)
        end
        points
      rescue => e
        @logger.error "InfluxDB: Error parsing output lines - #{e.backtrace}"
        @logger.error "InfluxDB: #{output}"
      end

      def run(event)
        series, output = parse_event(event)

        points = parse_output_lines(output)

        begin
          points.each do |metric, metric_points|
            @influxdb.write_point("#{series}.#{metric}", metric_points, true)
          end
        rescue => e
          @logger.error("InfluxDB: Error posting event - #{e.backtrace}")
        end

        yield('InfluxDB: Handler finished', 0)
      end
    end
  end
end
