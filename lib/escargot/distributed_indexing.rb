require 'sidekiq'

module Escargot

  module DistributedIndexing

    def DistributedIndexing.enqueue_all_records(model, index_version, options = {})
      options.reverse_merge!(:fields => :id)
      model.find_in_batches(options) do |batch|
        ids = batch.map(&:id)
        ids.map! { |id| model.serialize_id(id) } if model.respond_to?(:serialize_id)
        if Escargot.client.respond_to?(:bulk)
          BulkIndexDocuments.perform_async(model.to_s, ids, index_version)
        else
          IncrementalIndexDocuments.perform_async(model.to_s, ids, index_version)
        end
      end
    end

    #TODO move the code in TopicMap in here
    # give it a yield so I can set my other jobs off
    # and fix the rake tasks so they can call this
    
    # def DistributedIndexing.create_index_for_model(model)
      # #load_dependencies
      # index_version = model.create_index_version
      # enqueue_all_records(model, index_version)
      # DeployNewVersion.perform_async(model.index_name, index_version)
    # end

    class IncrementalIndexDocuments
      include Sidekiq::Worker
      sidekiq_options queue: "indexing"
      sidekiq_options retry: false
      sidekiq_options unique: true#, unique_job_expiration: 120 * 60 # 2 hours

      def perform(model_name, ids, index_version)
        model = model_name.constantize
        ids.map! { |id| model.deserialize_id(id) } if model.respond_to?(:deserialize_id)
        model = model_name.constantize
        model.all(:id => ids).each do |record|
          record.local_index_in_elastic_search(:index => index_version)
        end
      end
    end
    
    class BulkIndexDocuments
      include Sidekiq::Worker
      sidekiq_options queue: "indexing"
      sidekiq_options retry: false
      sidekiq_options unique: true#, unique_job_expiration: 120 * 60 # 2 hours
      
      def perform(model_name, ids, index_version)
        model = model_name.constantize
        ids.map! { |id| model.deserialize_id(id) } if model.respond_to?(:deserialize_id)
        Escargot.client.bulk do |bulk_client|
          model.all(:id => ids).each do |record|
            record.local_index_in_elastic_search(:index => index_version, :bulk_client => bulk_client)
          end
        end
      end
      
    end

    class RemoveDocuments
      include Sidekiq::Worker
      sidekiq_options queue: "nrt"
      sidekiq_options retry: true
      sidekiq_options unique: true#, unique_job_expiration: 120 * 60 # 2 hours

      def perform(model_name, ids, options = {})
        model = model_name.constantize
        ids.each do |id|
          model.delete_id_from_index(id, options)
        end
      end
    end

    class ReIndexDocuments
      include Sidekiq::Worker
      sidekiq_options queue: "nrt"
      sidekiq_options retry: true
      sidekiq_options unique: true#, unique_job_expiration: 120 * 60 # 2 hours

      def perform(model_name, ids)
        model = model_name.constantize
        ids.map! { |id| model.deserialize_id(id) } if model.respond_to?(:deserialize_id)
        ids_found = []
        model.all(:id => ids).each do |record|
          record.local_index_in_elastic_search
          ids_found << record.id
        end

        missing_ids = []
        ids.each { |id| missing_ids << id unless ids_found.any? { |found_id| found_id == id } }

        missing_ids.each do |id|
          model.delete_id_from_index(id)
        end
      end
    end
    
    class DeployNewVersion
      include Sidekiq::Worker
      sidekiq_options queue: "indexing"
      sidekiq_options retry: false
      sidekiq_options unique: true#, unique_job_expiration: 120 * 60 # 2 hours
            
      def perform(index, index_version, prune = false)
        Escargot.client.deploy_index_version("current_#{index}", index_version)
      end
    end

    class RetireOldVersion
      include Sidekiq::Worker
      sidekiq_options queue: "indexing"
      sidekiq_options retry: false
      sidekiq_options unique: true#, unique_job_expiration: 120 * 60 # 2 hours
      
      def perform(index, index_version, prune = false)
        Escargot.client.deploy_index_version("previous_#{index}", index_version)
        Escargot.client.prune_index_versions(index) if prune
      end
    end
    
  end

end
