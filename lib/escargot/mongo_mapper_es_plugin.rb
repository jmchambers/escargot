module MongoMapperEsPlugin
  extend ActiveSupport::Concern

  included do
    
    include Escargot::ActiveRecordExtensions

  end

  module ClassMethods
    
    def table_exists?
      true
    end
    
    def find_in_batches(options = {})
      
      query      = options.delete(:query)      || {}
      batch_size = options.delete(:batch_size) || 1000
      
      options.reverse_merge!(
        :timeout => :false, #true|false, false by default as we're commonly iterrating over whole collections
        #:fields  => [:_id]
      )
      
      query = where(query).criteria_hash #do some MM normalization
      
      collection.find(query, options) do |cursor|
        cursor.each_slice(batch_size) do |records|
          yield records.map { |record| load record }
        end
      end
      
    end

  end

end