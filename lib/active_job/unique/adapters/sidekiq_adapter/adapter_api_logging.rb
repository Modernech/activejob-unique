require 'active_support/concern'

module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module AdapterApiLogging
          extend ActiveSupport::Concern

          module ClassMethods
            def set_progress_stage_log_data(log_data_key, log_data_field, log_data)
              Sidekiq.redis_pool.with do |conn|
                conn.hsetnx(log_data_key, log_data_field, log_data)
              end
            end

            def incr_progress_stage_log_id_score(conn, job_score_key, base, new_id)
              if conn.zadd(job_score_key, [0, "#{base}:#{new_id}"], nx: true) == 1
                conn.zincrby(job_score_key, conn.zincrby(job_score_key, 1.0, base), "#{base}:#{new_id}").to_f
              else
                conn.zincrby(job_score_key, 0.0, "#{base}:#{new_id}").to_f
              end
            end

            def incr_progress_stage_log(day,
                                        job_score_key,
                                        queue_name,
                                        uniqueness_id,
                                        job_id,
                                        progress_stage_score,
                                        job_log_key,
                                        job_log_value)
              Sidekiq.redis_pool.with do |conn|
                job_id_value = "#{queue_name}:#{uniqueness_id}:#{job_id}"
                job_id_score = conn.zscore(job_score_key, job_id_value).to_f

                if job_id_score.zero?
                  day_score = ensure_job_stage_log_day_base(day)

                  queue_id_score = incr_progress_stage_log_id_score(conn, job_score_key, 'queue', queue_name)
                  queue_id_score = ensure_job_stage_log_queue_id_base(queue_id_score)

                  uniqueness_id_score = incr_progress_stage_log_id_score(conn, job_score_key, 'uniqueness_id', uniqueness_id)
                  uniqueness_id_score = ensure_job_stage_log_uniqueness_id_base(uniqueness_id_score)

                  # time_score with timezone
                  now = Time.now.in_time_zone(ActiveJob::Unique::Stats.timezone)
                  time_score = ((now - now.to_date.in_time_zone(ActiveJob::Unique::Stats.timezone)) / 10).to_i

                  job_id_score = day_score + queue_id_score + uniqueness_id_score + time_score

                  if conn.zadd(job_score_key, [job_id_score, job_id_value], nx: true) == 0
                    job_id_score = conn.zscore(job_score_key, job_id_value).to_f
                  end
                end

                job_log_score = job_id_score + progress_stage_score
                conn.zadd(job_log_key, [job_log_score, job_log_value], nx: true)
              end
            end

          end
          # end ClassMethods
        end
      end
    end
  end
end
