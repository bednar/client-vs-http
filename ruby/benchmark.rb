require 'optparse'
require 'ostruct'
require 'influxdb'
require 'influxdb2/client'

# Ruby benchmark
module Benchmark
  # Parser arguments
  class Parser
    def self.parse(args)
      options = OpenStruct.new
      options.type = 'CLIENT_RUBY_V1'
      options.threads_count = 200
      options.seconds_count = 60
      options.batch_size = 50_000
      options.flush_interval = 10_000
      options.line_protocols_count = 10
      options.skip_count = false
      options.measurement_name = "sensor_#{Time.new.to_i}"
      types = %w[CLIENT_RUBY_V1 CLIENT_RUBY_V2]

      opt_parser = OptionParser.new do |opts|
        opts.on('--type VALUE', types,
                'Type of writer (default "CLIENT_RUBY_V1"; CLIENT_RUBY_V1, CLIENT_RUBY_V2)') do |value|
          options.type = value
        end

        opts.on('--threadsCount VALUE', Integer, 'how much Thread use to write into InfluxDB') do |value|
          options.threads_count = value
        end

        opts.on('--secondsCount VALUE', Integer, 'how long write into InfluxDB') do |value|
          options.seconds_count = value
        end

        opts.on('--batchSize VALUE', Integer, 'batch size') do |value|
          options.batch_size = value
        end

        opts.on('--flushInterval VALUE', Integer, 'buffer flush interval') do |value|
          options.flush_interval = value
        end

        opts.on('--lineProtocolsCount VALUE', Integer, 'how much data writes in one batch') do |value|
          options.line_protocols_count = value
        end

        opts.on('--skipCount', 'skip query for counting rows on end of benchmark') do
          options.skip_count = true
        end

        opts.on('--measurementName VALUE', 'measurement name') do |value|
          options.measurement_name = value
        end

        opts.on('-?', '--help', 'Prints this help') do
          puts opts
        end
      end

      begin
        opt_parser.parse(args)
      rescue StandardError => e
        puts "Exception encountered: #{e}"
        exit 1
      end

      options
    end
  end

  # default writer
  #
  class Writer
    def initialize(influxdb_client)
      @client = influxdb_client
    end

    attr_reader :client
  end

  # Writer for InfluxDB V1
  #
  class WriterV1 < Writer
    def initialize(influxdb_client)
      super(influxdb_client)
    end

    def write(id, measurement_name:, iteration:)
      data = {id: id, values: {temprerature: rand(-10..35)}, timestamp: iteration}
      @client.write_point(measurement_name, data)
    end

    def count_rows(measurement_name)
      query = "select count(*) from #{measurement_name}"
      result = @client.query(query)
      result.empty? ? 0 : result[0]['values'][0]['count_temprerature']
    end
  end

  # Writer for InfluxDB V2
  #
  class WriterV2 < Writer
    def initialize(influxdb_client, batch_size:, flush_interval:)
      super(influxdb_client)

      @write_api = @client.create_write_api(
        write_options: InfluxDB2::WriteOptions.new(
          write_type: InfluxDB2::WriteType::BATCHING,
          batch_size: batch_size,
          flush_interval: flush_interval
        )
      )
    end

    def write(id, measurement_name:, iteration:)
      line = "#{measurement_name},id=#{id} temperature=#{rand(-10..35)} #{iteration}"
      @write_api.write(bucket: 'my-bucket', org: 'my-org', data: line)
    end

    def count_rows(measurement_name)
      query = "from(bucket:\"my-bucket\") \n
        |> range(start: 0, stop: now()) \n
        |> filter(fn: (r) => r._measurement == \"#{measurement_name}\") \n
        |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"],valueColumn: \"_value\") \n
        |> drop(columns: [\"id\", \"host\"]) \n
        |> count(column: \"temperature\")"

      result = @client.create_query_api.query(query: query, org: 'my-org')
      result.empty? ? 0 : result[0].records[0].values['temperature']
    end
  end

  # Worker which manages writing threads
  #
  class Worker
    attr_reader :threads

    def initialize(writer, options)
      @writer = writer
      @stop = false
      @options = options

      @threads = []
      @options.threads_count.times do |id|
        @threads << Thread.new do
          _work(id)
        end
      end
    end

    def stop!
      @stop = true
    end

    private

    def _work(id)
      start_time = Time.new.to_f

      (1..@options.seconds_count + 1).each do |i|
        iteration_time_start = Time.new.to_f

        break if Time.new.to_f > start_time + @options.seconds_count || @stop

        puts "writing iterations:: #{i}/#{@options.seconds_count}" if id == 0

        start = i * @options.line_protocols_count
        stop = start + @options.line_protocols_count

        (start..stop).each do |j|
          if Time.new.to_f > start_time + @options.seconds_count
            # puts "#{id} Time elapsed"
            break
          end

          break if Time.new.to_f > start_time + @options.seconds_count || @stop

          @writer.write(id, measurement_name: @options.measurement_name, iteration: j)

          break if @stop

          iteration_duration = Time.new.to_f - iteration_time_start.to_f
          # puts "iteration duration #{iteration_duration}s"

          sleep(1 - iteration_duration) if iteration_duration < 1
        end
      end
    end
  end

  class Client2 < InfluxDB2::Client
    def close!
      # do not flush queue
    end
  end

  options = Parser.parse(ARGV)

  expected = options.threads_count * options.seconds_count *
             options.line_protocols_count

  puts "\n"
  print(format("------------- #{options.type} -------------\n"))
  puts "\n"
  print("measurement:        #{options.measurement_name} \n")
  print("threadsCount:       #{options.threads_count} \n")
  print("secondsCount:       #{options.seconds_count} \n")
  print("lineProtocolsCount: #{options.line_protocols_count} \n")
  puts "\n"
  print("batchSize:          #{options.batch_size} \n")
  print("flushInterval:      #{options.flush_interval} \n")
  puts "\n"
  puts format("expected size:      #{expected} \n")
  puts "\n"

  writer = nil

  if options.type == 'CLIENT_RUBY_V2'
    writer = WriterV2.new(Client2.new('http://localhost:9999',
                                                'my-token',
                                                org: 'my-org',
                                                bucket: 'my-bucket',
                                                precision: InfluxDB2::WritePrecision::NANOSECOND,
                                                use_ssl: false),
                          batch_size: options.batch_size,
                          flush_interval: options.flush_interval)
  elsif options.type == 'CLIENT_RUBY_V1'
    async_options = {max_post_points: options.batch_size}
    writer = WriterV1.new(InfluxDB::Client.new(url: 'http://localhost:8086',
                                               database: 'iot_writes',
                                               user: 'root',
                                               password: 'root',
                                               async: async_options))
  end

  unless writer.nil?
    worker = Worker.new(writer, options)

    # sleep main thread
    sleep(options.seconds_count)

    # stop all threads
    puts "Stoping threads!\n"
    worker.stop!

    worker.threads.each do |t|
      t.join
    end

    puts "All threads finished"

    unless options.skip_count
      count = writer.count_rows(options.measurement_name)

      puts "\n"
      puts "Results:\n"
      puts "-> expected:        #{expected} \n"
      puts "-> total:           #{count} \n"
      puts "-> rate [%]:        #{(count.to_f / expected.to_f) * 100} \n"
      puts "-> rate [msg/sec]:  #{count / options.seconds_count} \n"

      puts "Written records #{options.type}:#{count}"
    end
  end
end
