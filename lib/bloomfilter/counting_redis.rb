module BloomFilter
  class CountingRedis < Filter

    def initialize(opts = {})
      @opts = {
        :size    => 100,
        :hashes  => 4,
        :seed    => Time.now.to_i,
        :bucket  => 3,
        :ttl => false,
        :server => {}
      }.merge opts
      @db = @opts.delete(:db) || ::Redis.new(@opts[:server])
    end

    def insert(key, ttl=nil)
      ttl = @opts[:ttl] if ttl.nil?

      indexes_for(key).each do |idx|
        @db.incr idx
        @db.expire(idx, ttl) if ttl
      end
    end
    alias :[]= :insert

    def delete(key)
      indexes_for(key).each do |idx|
        if @db.decr(idx).to_i <= 0
          @db.del(idx)
        end
      end
    end

    def include?(*keys)
      indexes = keys.collect { |key| indexes_for(key) }
      not @db.mget(*indexes.flatten).include? nil
    end
    alias :key? :include?

    def num_set
      @db.keys("rbloom:*").size
    end
    alias :size :num_set

    def clear
      @db.flushdb
    end

    # return true if insertion is success.
    # retrun false if #include? is true.
    def insert_if_not_included(key)
        indexes = indexes_for(key)
        need_retry = false
        result = false
        begin
            @db.watch indexes do
                if self.include?(key)
                    need_retry = false
                    result = false
                else
                    need_retry = @db.multi do
                        self.insert(key)
                    end
                    result = true unless need_retry.nil?
                end
            end
        end while need_retry.nil?

        result
    end

    private

      # compute index offsets for provided key
      def indexes_for(key)
        indexes = []
        @opts[:hashes].times do |i|
          indexes.push "rbloom:" + (Zlib.crc32("#{key}:#{i+@opts[:seed]}") % @opts[:size]).to_s
        end

        indexes
      end
  end
end
