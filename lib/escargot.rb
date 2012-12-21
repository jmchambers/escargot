require 'elasticsearch'
require 'escargot/activerecord_ex'
require 'escargot/elasticsearch_ex'
require 'escargot/mongo_mapper_es_plugin'
require 'escargot/enumerable_ex'
require 'escargot/local_indexing'
require 'escargot/distributed_indexing'
require 'escargot/queue_backend/base'
require 'escargot/queue_backend/resque'

module Escargot
  def self.register_model(model)
    return unless model.table_exists?
    @indexed_models ||= []
    @indexed_models.delete(model) if @indexed_models.include?(model)
    @indexed_models << model
  end

  def self.client=(new_connection)
    @@client = new_connection
  end
  
  def self.client
    Thread.current[:escargot_client] ||= ElasticSearch.new("http://localhost:9200")
  end

  def self.indexed_models
    @indexed_models || []
  end

  def self.queue_backend
    @queue ||= Escargot::QueueBackend::Rescue.new
  end
  
  def self.flush_all_indexed_models
    @indexed_models = []
  end

  # search_hits returns a raw ElasticSearch::Api::Hits object for the search results
  # see #search for the valid options
  def self.search_hits(query, options = {}, call_by_instance_method = false)
    unless call_by_instance_method
      if (options[:classes])
        models = Array(options[:classes])
      else
        register_all_models
        models = @indexed_models
      end
      options = options.merge({:index => models.map(&:current_search_index_name).join(',')})
    end
    
    if query.kind_of?(Hash)
      query_dsl = query.delete(:query_dsl)
      query = {:query => query} if (query_dsl.nil? || query_dsl)
    end
    Escargot.client.search(query, options)
  end

  # search returns a will_paginate collection of ActiveRecord objects for the search results
  #
  # see ElasticSearch::Api::Index#search for the full list of valid options
  #
  # note that the collection may include nils if ElasticSearch returns a result hit for a
  # record that has been deleted on the database  
  def self.search(query, options = {}, call_by_instance_method = false)
    hits = Escargot.search_hits(query, options, call_by_instance_method)
    records = find_hits_in_db(hits)
    results = WillPaginate::Collection.new(hits.current_page, hits.per_page, hits.total_entries)
    results.replace(records)
    results
  end
  
  def self.group_hits_by_type(hits)
    hits.each_with_object(Hash.new { |hash, type| hash[type] = [] }) do |hit, hash|
      hash[hit._type] << hit
    end
  end
  
  def self.find_hits_in_db(hits)
    #fetch records from db in one call and then reorder to match search result ordering
    return Array.new if hits.empty?
    
    hits_by_type = group_hits_by_type(hits)
    
    unordered_records = hits_by_type.map do |type, hits_of_type|
      model_class = get_model_class_from_hit_type(type)
      ids = hits_of_type.map(&:_id)
      ids.map! { |id| model_class.deserialize_id(id) } if model_class.respond_to?(:deserialize_id)
      model_class.find! ids
    end.flatten
    
    ranked_ids = hits.map(&:_id)

    if unordered_records.is_a?(Array)
      records = unordered_records.reorder_by(ranked_ids, &Proc.new {|r| r.id.to_s})
    elsif unordered_records.nil?
      records = []
    else
      records = [unordered_records]
    end
    records
  end
  
  def self.get_model_class_from_hit_type(type)
    model_class = type.gsub(/-/,'/').classify.constantize
  end

  # counts the number of results for this query.
  def self.search_count(query = "*", options = {}, call_by_instance_method = false)
    unless call_by_instance_method
      if (options[:classes])
        models = Array(options[:classes])
      else
        register_all_models
        models = @indexed_models
      end
      options = options.merge({:index => models.map(&:current_search_index_name).join(',')})
    end
    Escargot.client.count(query, options)
  end

  private
    def self.register_all_models
      models = []
      # Search all Models in the application Rails
      Dir[File.join(Rails.root.to_s + "/app/models", "**", "*.rb")].each do |file|
        model = file.gsub(/#{Rails.root.to_s}\/app\/models\/(.*?)\.rb/,'\1').classify.constantize
        # unless models.include?(model)
          # require file
        # end
        models << model
      end
    end


end


#-------------------------------------------------------------------------------
require 'escargot' 
require 'escargot/rails/init_commun'

# preserve rails 2.x compatibility
(Rails::VERSION::MAJOR == 3) ? (require 'escargot/rails/railtie') : (require 'escargot/rails/init')
#-------------------------------------------------------------------------------
