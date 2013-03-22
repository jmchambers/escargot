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
      
      query      = options[:query]      || {}
      batch_size = options[:batch_size] || 1000
      fields     = *options[:fields]
      
      where(query).fields(fields).each_slice(batch_size) do |records|
        yield records
      end
      
    end

  end

end