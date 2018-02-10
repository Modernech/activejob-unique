require 'active_support/concern'
require 'sidekiq/api'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          JOB_PROGRESS_ENQUEUE_ATTEMPTED = :enqueue_attempted
          JOB_PROGRESS_ENQUEUE_PROCESSING = :enqueue_processing
          JOB_PROGRESS_ENQUEUE_FAILED = :enqueue_failed
          JOB_PROGRESS_ENQUEUE_PROCESSED = :enqueue_processed
          JOB_PROGRESS_ENQUEUE_SKIPPED = :enqueue_skipped

          JOB_PROGRESS_PERFORM_ATTEMPTED = :perform_attempted
          JOB_PROGRESS_PERFORM_PROCESSING = :perform_processing
          JOB_PROGRESS_PERFORM_FAILED = :perform_failed
          JOB_PROGRESS_PERFORM_PROCESSED = :perform_processed
          JOB_PROGRESS_PERFORM_SKIPPED = :perform_skipped

          UNIQUENESS_MODE_WHILE_EXECUTING = :while_executing
          UNIQUENESS_MODE_UNTIL_TIMEOUT = :until_timeout
          UNIQUENESS_MODE_UNTIL_EXECUTING = :until_executing
          UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING = :until_and_while_executing

          def ensure_data_utf8(data)
            data.to_s.encode('utf-8', invalid: :replace, undef: :replace, replace: '')
          end

          def sequence_today
            Time.now.utc.to_date.strftime('%Y%m%d').to_i
          end

          def enqueue_stage?(progress)
            [JOB_PROGRESS_ENQUEUE_ATTEMPTED,
             JOB_PROGRESS_ENQUEUE_PROCESSING,
             JOB_PROGRESS_ENQUEUE_PROCESSED,
             JOB_PROGRESS_ENQUEUE_FAILED,
             JOB_PROGRESS_ENQUEUE_SKIPPED].include?(progress.to_s.to_sym)
          end

          def perform_stage?(progress)
            [JOB_PROGRESS_PERFORM_ATTEMPTED,
             JOB_PROGRESS_PERFORM_PROCESSING,
             JOB_PROGRESS_PERFORM_PROCESSED,
             JOB_PROGRESS_PERFORM_FAILED,
             JOB_PROGRESS_PERFORM_SKIPPED].include?(progress.to_s.to_sym)
          end

          def unknown_stage?(progress)
            !enqueue_stage?(progress) && !perform_stage?(progress)
          end

          def enqueue_stage_job?(uniqueness_id, queue_name)
            j = JSON.load(read_uniqueness(uniqueness_id, queue_name)) rescue nil
            return false if j.blank?

            enqueue_stage?(j['p'])
          end

          def perform_stage_job?(uniqueness_id, queue_name)
            j = JSON.load(read_uniqueness(uniqueness_id, queue_name)) rescue nil
            return false if j.blank?

            perform_stage?(j['p'])
          end

          def dirty_uniqueness?(uniqueness_id, uniqueness, queue_name)
            j = JSON.load(uniqueness) rescue nil
            return true if j.blank?

            now = Time.now.utc.to_i

            # progress, timeout, expires
            progress = j['p']
            timeout = j['t'].to_i
            expires = j['e'].to_i

            # when default expiration passed
            return true if expires < now

            # expiration passed
            return true if timeout < now

            # unknown stage
            return true if unknown_stage?(progress)

            false
          end

          def cleanable_uniqueness?(uniqueness_id, uniqueness, queue_name)
            j = JSON.load(uniqueness) rescue nil
            return true if j.blank?

            now = Time.now.utc.to_i

            # progress, timeout, expires
            progress = j['p']
            expires = j['e'].to_i

            # when default expiration passed
            return true if expires < now

            # unknown stage
            return true if unknown_stage?(progress)

            false
          end

          def read_uniqueness(uniqueness_id, queue_name)
            uniqueness = nil

            Sidekiq.redis_pool.with do |conn|
              uniqueness = conn.hget("uniqueness:#{queue_name}", uniqueness_id)
            end

            uniqueness
          end

          def read_uniqueness_dump(uniqueness_id, queue_name)
            uniqueness = nil

            Sidekiq.redis_pool.with do |conn|
              uniqueness = conn.hget("uniqueness:dump:#{queue_name}", uniqueness_id)
            end

            uniqueness
          end

          def write_uniqueness_progress(uniqueness_id, queue_name, klass, uniqueness_mode, progress, timeout, expires)
            # expires must be later than timeout
            expires += 5.minutes if expires < timeout

            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:#{queue_name}", uniqueness_id, JSON.dump("k": klass, "m": uniqueness_mode, "p": progress, "t": timeout, "e": expires, "u": Time.now.utc.to_i))
            end
          end

          def write_uniqueness_dump(uniqueness_id, queue_name, klass, args, job_id)
            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:dump:#{queue_name}", uniqueness_id, JSON.dump("k": klass, "a": args, "j": job_id))
            end
          end

          def write_uniqueness_progress_and_dump(uniqueness_id, queue_name, klass, args, job_id, uniqueness_mode, progress, timeout, expires)
            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:#{queue_name}", uniqueness_id, JSON.dump("k": klass, "a": args, "j": job_id, "m": uniqueness_mode, "p": progress, "t": timeout, "e": expires, "u": Time.now.utc.to_i))
            end
          end

          def clean_uniqueness(uniqueness_id, queue_name)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                conn.hdel("uniqueness:#{queue_name}", uniqueness_id)
                conn.hdel("uniqueness:dump:#{queue_name}", uniqueness_id)
              end
            end
          end

          def cleanup_uniqueness_timeout(limit = 1000)
            queue_names = Sidekiq::Queue.all.map(&:name)
            output = {}

            Sidekiq.redis_pool.with do |conn|
              queue_names.each do |name|
                next if (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).blank?
                output[name] = 0
                cursor = '0'

                loop do
                  cursor, fields = conn.hscan("uniqueness:#{name}", cursor, count: 100)

                  fields.each do |uniqueness_id, uniqueness|
                    if cleanable_uniqueness?(uniqueness_id, uniqueness, name)
                      clean_uniqueness(uniqueness_id, name)
                      output[name] += 1
                    end
                  end

                  break if cursor == '0'
                  break if output[name] >= limit
                end
              end
            end

            output
          end

          def cleanup_uniqueness_all(limit = 10_000)
            queue_names = Sidekiq::Queue.all.map(&:name)
            output = {}

            Sidekiq.redis_pool.with do |conn|
              queue_names.each do |name|
                next if (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).blank?
                output[name] = 0
                cursor = '0'

                loop do
                  cursor, fields = conn.hscan("uniqueness:#{name}", cursor, count: 100)

                  fields.each do |uniqueness_id, _uniqueness|
                    clean_uniqueness(uniqueness_id, name)
                    output[name] += 1
                  end

                  break if cursor == '0'
                  break if output[name] >= limit
                end
              end
            end

            output
          end

          def incr_job_stats(queue_name, klass, progress)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                conn.hsetnx("jobstats:#{sequence_today}:#{progress}:#{queue_name}", klass, 0)
                conn.hincrby("jobstats:#{sequence_today}:#{progress}:#{queue_name}", klass, 1)
              end
            end
          end

          def sync_overall_stats(range = 1)
            today = sequence_today
            to = today - 1
            from = to - range

            queue_names = Sidekiq::Queue.all.map(&:name)
            output = {}

            Sidekiq.redis_pool.with do |conn|
              queue_names.each do |name|
                next if (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).blank?
                output[name] = 0

                (from..to).each do |day|
                  %i[enqueue perform].each do |stage|
                    klasses = conn.hkeys("jobstats:#{day}:#{stage}_attempted:#{name}")

                    klasses.each do |klass|
                      %i[attempted skipped processing failed processed].each do |progress|
                        val = conn.hget("jobstats:#{day}:#{stage}_#{progress}:#{name}", klass).to_i
                        if val.positive?
                          conn.hsetnx("jobstats:#{stage}_#{progress}:#{name}", klass, 0)
                          conn.hincrby("jobstats:#{stage}_#{progress}:#{name}", klass, val)
                        end

                        conn.hdel("jobstats:#{day}:#{stage}_#{progress}:#{name}", klass)
                      end
                    end
                  end
                end
              end
            end
          end
        end

        def sequence_today
          self.class.sequence_today
        end

        def enqueue_stage?(*args)
          self.class.enqueue_stage?(*args)
        end

        def perform_stage?(*args)
          self.class.perform_stage?(*args)
        end

        def enqueue_stage_job?(*args)
          self.class.enqueue_stage_job?(*args)
        end

        def perform_stage_job?(*args)
          self.class.perform_stage_job?(*args)
        end

        def dirty_uniqueness?(*args)
          self.class.dirty_uniqueness?(*args)
        end

        def read_uniqueness(*args)
          self.class.read_uniqueness(*args)
        end

        def read_uniqueness_dump(*args)
          self.class.read_uniqueness_dump(*args)
        end

        def write_uniqueness_dump(*args)
          self.class.write_uniqueness_dump(*args)
        end

        def write_uniqueness_progress(*args)
          self.class.write_uniqueness_progress(*args)
        end

        def clean_uniqueness(*args)
          self.class.clean_uniqueness(*args)
        end

        def cleanup_uniqueness_timeout(*args)
          self.class.cleanup_uniqueness_timeout(*args)
        end

        def cleanup_uniqueness_all(*args)
          self.class.cleanup_uniqueness_all(*args)
        end

        def incr_job_stats(*args)
          self.class.incr_job_stats(*args)
        end

        def sync_overall_stats(*args)
          self.class.sync_overall_stats(*args)
        end
      end
    end
  end
end

ActiveJob::QueueAdapters::SidekiqAdapter.send(:include, ActiveJob::Unique::QueueAdapters::SidekiqAdapter)
