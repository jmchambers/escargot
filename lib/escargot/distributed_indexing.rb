require 'sidekiq'

module Escargot

  module DistributedIndexing

    def DistributedIndexing.enqueue_all_records(model, index_version, options = {})
      options.reverse_merge!(:fields => :id)
      model.find_in_batches(options) do |batch|
        ids = model.serialize_ids(batch) if model.respond_to?(:serialize_ids)
        if Escargot.client.respond_to?(:bulk)
          BulkIndexDocuments.perform_async(model.to_s, ids, index_version)
        else
          IncrementalIndexDocuments.perform_async(model.to_s, ids, index_version)
        end
      end
    end

    def DistributedIndexing.create_index_for_model(model)
      #load_dependencies
      index_version = model.create_index_version
      enqueue_all_records(model, index_version)
      DeployNewVersion.perform_async(model.index_name, index_version)
    end

    class IncrementalIndexDocuments
      include Sidekiq::Worker
      sidekiq_options queue: "indexing"
      sidekiq_options retry: false

      def perform(model_name, ids, index_version)
        ids   = model.deserialize_ids(ids) if model.respond_to?(:deserialize_ids)
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

      def perform(model_name, ids, index_version)
        model = model_name.constantize
        ids   = model.deserialize_ids(ids) if model.respond_to?(:deserialize_ids)
        Escargot.client.bulk do |bulk_client|
          model.all(:id => ids).each do |record|
            record.local_index_in_elastic_search(:index => index_version, :bulk_client => bulk_client)
          end
        end
      end
      
    end

    class ReIndexDocuments
      include Sidekiq::Worker
      sidekiq_options queue: "nrt"
      sidekiq_options retry: false

      def perform(model_name, ids)
        model = model_name.constantize
        ids   = model.deserialize_ids(ids) if model.respond_to?(:deserialize_ids)
        ids_found = []
        model.all(:id => ids).each do |record|
          record.local_index_in_elastic_search
          ids_found << record.id
        end

        (ids - ids_found).each do |id|
          model.delete_id_from_index(id)
        end
      end
    end

    class DeployNewVersion
      include Sidekiq::Worker
      sidekiq_options queue: "indexing"
      sidekiq_options retry: false
      
      def perform(index, index_version, prune = false)
        Escargot.client.deploy_index_version(index, index_version)
        Escargot.client.prune_index_versions(index) if prune
      end
    end
    
  end

end
