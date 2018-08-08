class ActiveJobs::WithPerformOnlyJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_perform_only
  unique_for :while_executing

  def perform(args, *_args)
    run(args)

    true
  end

  def self.enqueue_multiple
    total = 10

    self.prepare_multiple(total)

    (1..100).each do
      (1..total).to_a.shuffle.each do |args|
        ActiveJobs::EnqueueJob.perform_later(self.name, [args])
      end
    end
    
    true
  end
end
