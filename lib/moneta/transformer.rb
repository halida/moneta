module Moneta
  # Transforms keys and values (Marshal, YAML, JSON, Base64, MD5, ...).
  # You can bypass the transformer (e.g. serialization) by using the `:raw` option.
  #
  # @example Add `Moneta::Transformer` to proxy stack
  #   Moneta.build do
  #     transformer key: [:marshal, :escape], value: [:marshal]
  #     adapter :File, dir: 'data'
  #   end
  #
  # @example Bypass serialization
  #   store.store('key', 'value', raw: true)
  #   store['key'] # raises an Exception
  #   store.load('key', raw: true) # returns 'value'
  #
  #   store['key'] = 'value'
  #   store.load('key', raw: true) # returns "\x04\bI\"\nvalue\x06:\x06ET"
  #
  # @api public
  class Transformer < Proxy
    class << self
      alias_method :original_new, :new

      # @param [Moneta store] adapter The underlying store
      # @param [Hash] options
      # @return [Transformer] new Moneta transformer
      # @option options [Array] :key List of key transformers in the order in which they should be applied
      # @option options [Array] :value List of value transformers in the order in which they should be applied
      # @option options [String] :prefix Prefix string for key namespacing (Used by the :prefix key transformer)
      # @option options [String] :secret HMAC secret to verify values (Used by the :hmac value transformer)
      # @option options [Integer] :maxlen Maximum key length (Used by the :truncate key transformer)
      def new(adapter, options = {})
        keys = [options[:key]].flatten.compact
        values = [options[:value]].flatten.compact
        raise ArgumentError, 'Option :key or :value is required' if keys.empty? && values.empty?
        options[:prefix] ||= '' if keys.include?(:prefix)
        name = class_name(keys, values)
        const_set(name, compile(keys, values)) unless const_defined?(name)
        const_get(name).original_new(adapter, options)
      end

      private

      def compile(keys, values)
        @key_validator ||= compile_validator(KEY_TRANSFORMER)
        @value_validator ||= compile_validator(VALUE_TRANSFORMER)

        raise ArgumentError, 'Invalid key transformer chain' if @key_validator !~ keys.map(&:inspect).join
        raise ArgumentError, 'Invalid value transformer chain' if @value_validator !~ values.map(&:inspect).join

        klass = Class.new(self)
        klass.class_eval <<-end_eval, __FILE__, __LINE__
          def initialize(adapter, options = {})
            super
            #{compile_initializer('key', keys)}
            #{compile_initializer('value', values)}
          end
        end_eval

        compile_key_value_transformer(klass, keys, values)

        klass
      end

      def without(*options)
        options = options.flatten.uniq
        options.empty? ? 'options' : "Utils.without(options, #{options.map(&:to_sym).map(&:inspect).join(', ')})"
      end

      def compile_key_value_transformer(klass, keys, values)
        key, key_opts = compile_transformer(keys, 'key')
        key_load, key_load_opts = compile_transformer(keys.reverse, 'key', 1)
        dump, dump_opts = compile_transformer(values, 'value')
        load, load_opts = compile_transformer(values.reverse, 'value', 1)

        dump = "(options[:raw] ? value : #{dump})"
        load = "(value && !options[:raw] ? #{load} : value)"
        load_opts << :raw
        dump_opts << :raw

        all_opts = [key_opts, key_load_opts, dump_opts, load_opts]

        klass.class_eval <<-end_eval, __FILE__, __LINE__
          def key?(key, options = {})
            @adapter.key?(#{key}, #{without key_opts, key_load_opts})
          end
          def increment(key, amount = 1, options = {})
            @adapter.increment(#{key}, amount, #{without key_opts, key_load_opts})
          end
          def load(key, options = {})
            value = @adapter.load(#{key}, #{without key_opts, key_load_opts, load_opts})
            #{load}
          end
          def store(key, value, options = {})
            @adapter.store(#{key}, #{dump}, #{without key_opts, key_load_opts, dump_opts})
            value
          end
          def delete(key, options = {})
            value = @adapter.delete(#{key}, #{without key_opts, key_load_opts, load_opts})
            #{load}
          end
          def create(key, value, options = {})
            @adapter.create(#{key}, #{dump}, #{without key_opts, key_load_opts, dump_opts})
          end
          def each_keys(options = {})
            return to_enum(:each_keys) unless block_given?
            @adapter.each_keys(#{without key_opts, dump_opts, load_opts}) do |key|
              yield(#{key_load})
            end
          end
          def each_values(options = {})
            return to_enum(:each_values) unless block_given?
            @adapter.each_values(#{without key_opts, key_load_opts, dump_opts}) do |value|
              yield(#{load})
            end
          end
          def keys(options = {})
            self.each_keys(options).to_a
          end
          def values(options = {})
            self.each_values(options).to_a
          end
          def each(options = {})
            return to_enum(:each) unless block_given?
            @adapter.each(#{without key_opts, dump_opts}) do |key, value|
              key = (#{key_load})
              value = (#{load})
              yield([key, value])
            end
          end
          def all(options = {})
            self.each(options).to_a
          end
        end_eval
      end

      # Compile option initializer
      def compile_initializer(type, transformers)
        transformers.map do |name|
          t = TRANSFORMER[name]
          (t[1].to_s + t[2].to_s).scan(/@\w+/).uniq.map do |opt|
            "raise ArgumentError, \"Option #{opt[1..-1]} is required for #{name} #{type} transformer\" unless #{opt} = options[:#{opt[1..-1]}]\n"
          end
        end.join("\n")
      end

      def compile_validator(s)
        Regexp.new('\A' + s.gsub(/\w+/) do
                     '(' + TRANSFORMER.select {|k,v| v.first.to_s == $& }.map {|v| ":#{v.first}" }.join('|') + ')'
                   end.gsub(/\s+/, '') + '\Z')
      end

      # Returned compiled transformer code string
      def compile_transformer(transformer, var, i = 2)
        value, options = var, []
        transformer.each do |name|
          raise ArgumentError, "Unknown transformer #{name}" unless t = TRANSFORMER[name]
          require t[3] if t[3]
          code = t[i]
          raise ArgumentError, "Transformer #{name} don't have code #{i} : #{t}" unless code.present?
          options += code.scan(/options\[:(\w+)\]/).flatten
          value =
            if t[0] == :serialize && var == 'key' && i == 2
              "(tmp = #{value}; String === tmp ? tmp : #{code % 'tmp'})"
            else
              code % value
            end
        end
        return value, options
      end

      def class_name(keys, values)
        (keys.empty? ? '' : keys.map(&:to_s).map(&:capitalize).join + 'Key') +
          (values.empty? ? '' : values.map(&:to_s).map(&:capitalize).join + 'Value')
      end
    end
  end
end

require 'moneta/transformer/helper'
require 'moneta/transformer/config'
