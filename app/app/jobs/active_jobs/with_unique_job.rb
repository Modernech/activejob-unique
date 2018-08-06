class ActiveJobs::WithUniqueJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_unique
  unique_for true

  def perform(args, *_args)
    run(args)

    true
  end

  def self.enqueue_multiple
    total = 100

    self.prepare_multiple(total)

    (1..100).each do
      (1..total).to_a.shuffle.each do |args|
        ActiveJobs::EnqueueJob.perform_later(self.name, [args])
      end
    end

    true
  end
end