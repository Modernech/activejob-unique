module ActiveJob
  module Unique
    module Adapters
      module SidekiqAdapter
        module Web
          module Server
            def self.registered(app)
              # index page of stats
              app.get '/job_stats' do
                redirect '/job_stats/*/*'
              end

              app.get '/job_stats/:job_prefix/:queue_name' do
                view_path = File.join(File.expand_path(__dir__), 'views')

                @today = WebApi.sequence_today

                @job_prefix = route_params[:job_prefix].to_s
                @queue_name = route_params[:queue_name].to_s

                @job_stats = []
                @job_stats_today = {}
                @job_stats_all_time = {}
                @job_log_keys = {}
                @uniqueness_flag_keys = {}
                @count = (params[:count] || 10).to_i
                @current_page = params[:page].to_i
                @current_page = 1 if @current_page < 1

                begin_index = (@current_page - 1) * @count
                end_index = begin_index + @count - 1

                Sidekiq.redis_pool.with do |conn|
                  if @job_prefix == '*' && @queue_name == '*'
                    @total_size = conn.zcard(WebApi.job_progress_stats_jobs)
                    @job_stats = conn.zrevrange(WebApi.job_progress_stats_jobs, begin_index, end_index)
                  else
                    @job_stats = conn.zrevrange(WebApi.job_progress_stats_jobs, 0, -1)
                    @job_stats.reject!{|job| (job =~ /^#{@job_prefix}/i) != 0 } if @job_prefix != '*'
                    @job_stats = WebApi.query_job_progress_stats_job_names(@job_stats, @queue_name, @current_page) if @queue_name != '*'

                    @total_size = @job_stats.size
                    @job_stats = @job_stats[begin_index..end_index]
                  end
                end

                @job_stats_all_time = WebApi.regroup_job_progress_stats(@job_stats, @queue_name)
                @job_stats_today = WebApi.regroup_job_progress_stats_today(@job_stats, @queue_name)

                @uniqueness_flag_keys = WebApi.group_job_progress_stage_uniqueness_flag_keys(@job_stats)
                @processing_flag_keys = WebApi.group_job_progress_stage_processing_flag_keys(@job_stats)

                @job_log_keys = WebApi.group_job_progress_stage_log_keys(@job_stats_all_time)

                render(:erb, File.read(File.join(view_path, 'index.erb')))
              end

              app.get '/job_stats/uniqueness/:job_name/:queue_name/:stage' do
                view_path = File.join(File.expand_path(__dir__), 'views')

                @job_name = route_params[:job_name]
                @queue_name = route_params[:queue_name]
                @stage = route_params[:stage]
                @job_stats = []

                @count = (params[:count] || 25).to_i
                @current_page = params[:page].to_i
                @current_page = 1 if @current_page < 1

                @begin_index = (@current_page - 1) * @count
                next_page_availabe = false

                next_page_availabe, @job_stats = WebApi.query_job_progress_stage_state_uniqueness(
                  @job_name,
                  @queue_name,
                  @stage,
                  @count,
                  @begin_index
                )

                @total_size = @count * (@current_page - 1) + @job_stats.size
                @total_size += 1 if next_page_availabe

                render(:erb, File.read(File.join(view_path, 'uniqueness.erb')))
              end

              # delete single uniqueness flag
              app.post '/job_stats/uniqueness/:job_name/:queue_name/:stage/:uniqueness_id/delete' do
                job_name = route_params[:job_name]
                queue_name = route_params[:queue_name]
                uniqueness_id = route_params[:uniqueness_id].to_s
                stage = route_params[:stage]

                WebApi.cleanup_job_progress_state_uniqueness(
                  job_name,
                  queue_name,
                  stage,
                  uniqueness_id
                )

                redirect URI(request.referer).path
              end

              app.get '/job_stats/processing/:job_name/:queue_name/:uniqueness_id' do
                view_path = File.join(File.expand_path(__dir__), 'views')

                @job_name = route_params[:job_name]
                @queue_name = route_params[:queue_name]
                @uniqueness_id = route_params[:uniqueness_id]
                @job_stats = []

                @count = (params[:count] || 25).to_i
                @current_page = params[:page].to_i
                @current_page = 1 if @current_page < 1

                @begin_index = (@current_page - 1) * @count
                next_page_availabe = false

                next_page_availabe, @job_stats = WebApi.query_job_progress_stage_state_processing(
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @count,
                  @begin_index
                )

                @total_size = @count * (@current_page - 1) + @job_stats.size
                @total_size += 1 if next_page_availabe

                render(:erb, File.read(File.join(view_path, 'processing.erb')))
              end

              # delete single processing flag
              app.post '/job_stats/processing/:job_name/:queue_name/:uniqueness_id/:stage/delete' do
                job_name = route_params[:job_name]
                queue_name = route_params[:queue_name]
                uniqueness_id = route_params[:uniqueness_id].to_s
                stage = route_params[:stage].to_s

                WebApi.cleanup_job_progress_state_processing(
                  job_name,
                  queue_name,
                  uniqueness_id,
                  stage
                )

                redirect URI(request.referer).path
              end

              app.get '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id' do
                view_path = File.join(File.expand_path(__dir__), 'views')

                @job_logs = []
                @job_name = route_params[:job_name]
                @queue_name = route_params[:queue_name].to_s
                @day = route_params[:day].to_i
                @day = WebApi.sequence_today if @day < WebApi.sequence_day(Time.now.in_time_zone(ActiveJob::Unique::Stats.timezone) - 7 * ONE_DAY_SECONDS)

                @uniqueness_id = route_params[:uniqueness_id].to_s
                @uniqueness_id = '*' unless @uniqueness_id.size == 32

                @count = (params[:count] || 25).to_i
                @current_page = params[:page].to_i
                @current_page = 1 if @current_page < 1

                @begin_index = (@current_page - 1) * @count
                next_page_availabe = false

                next_page_availabe, @job_logs = WebApi.query_job_progress_stage_log_jobs(
                  @day,
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @count,
                  @begin_index
                )

                @total_size = @count * (@current_page - 1) + @job_stats.size
                @total_size += 1 if next_page_availabe

                render(:erb, File.read(File.join(view_path, 'logs.erb')))
              end

              app.get '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/:job_id' do
                view_path = File.join(File.expand_path(__dir__), 'views')

                @job_log = {}
                @job_name = route_params[:job_name]
                @queue_name = route_params[:queue_name]
                @day = route_params[:day].to_i
                @uniqueness_id = route_params[:uniqueness_id].to_s
                @job_id = route_params[:job_id]

                @job_log = WebApi.query_job_progress_stage_log_job_one(
                  @day,
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @job_id
                )

                render(:erb, File.read(File.join(view_path, 'log.erb')))
              end

              # delete all uniqueness logs
              app.post '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/delete' do
                job_name = route_params[:job_name]
                queue_name = route_params[:queue_name]
                day = route_params[:day].to_i
                uniqueness_id = route_params[:uniqueness_id].to_s

                WebApi.cleanup_job_progress_stage_logs(
                  day,
                  job_name,
                  queue_name,
                  uniqueness_id
                )

                redirect URI(request.referer).path
              end

              # delete single uniqueness log
              app.post '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/:job_id/delete' do
                job_name = route_params[:job_name]
                queue_name = route_params[:queue_name]
                day = route_params[:day].to_i
                uniqueness_id = route_params[:uniqueness_id].to_s
                job_id = route_params[:job_id]

                WebApi.cleanup_job_progress_state_log_one(
                  day,
                  job_name,
                  queue_name,
                  uniqueness_id,
                  job_id
                )

                redirect URI(request.referer).path
              end

              app.get '/job_stats/cleanup' do
                WebApi.cleanup_expired_progress_stats
                WebApi.cleanup_expired_progress_state_uniqueness
                WebApi.cleanup_expired_progress_stage_logs

                json({cleanup: 'OK'})
              end
            end
          end
        end
      end
    end
  end
end

if defined?(Sidekiq::Web)
  Sidekiq::Web.register ActiveJob::Unique::Adapters::SidekiqAdapter::Web::Server

  if Sidekiq::Web.tabs.is_a?(Array)
    # For sidekiq < 2.5
    Sidekiq::Web.tabs << 'Job Stats'
  else
    Sidekiq::Web.tabs['Job Stats'] = 'job_stats'
  end
end
