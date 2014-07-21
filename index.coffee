# Agent

# AWS key/secret
# do(message) -> decorated Q Promise
# notify(), error(), then(), etc. + start(), stop(), pause()

Q = require('Q')
Queues = require('lib/queues.coffee')
Threads = require('lib/thread.coffee')

Agent = (fn, config) ->

  # Config contains Queue configuration
  config ?= @config
  config ?= {}

  deferred = Q.defer()
  promise = deferred.promise

  # Cached (or lazily loaded) references
  @queue = () ->
    null
  @thread = () ->
    null

  # This method returns a array pair of types [ Thread, Queue ]
  @agent = () ->
    deferred = Q.defer()
    # Pull out relevant vars from config
    # Determine Thread fingerprint and state
    # If Thread not yet intialized:
      # If Queue not yet initialized:
        # Determined Queue name and initialization state
        # If Queue w/thread name not yet initialized:
          # Check if queue with task name already exists (first memory cache then AWS)
          # If task queue doesn't yet exist in memory or on AWS:
          # Create Queue from thread name
          # Create a new Thread, passing it queue ARN
          # Return [ Thread, Queue ]
        # Else, return [ Thread, Queue ]e
      # Else, return [ Thread, Queue ]
    # Else, return [ Thread, Queue ]
    deferred.promise

  # Begins a thread
  @start = () ->
    Thread = @thread()
    console.log('Begins a thread, preserving any stats', Thread)
    deferred = Q.defer()
    Thread.start().then( () ->
      Thread.startStats().then( () ->
        deferred.resolve()
      )
    )
    deferred.promise

  # Stops a thread, preserving queues and stats
  @pause = () ->
    Thread = @thread()
    console.log('Stops a thread, preserving queues and stats', Thread)
    deferred = Q.defer()
    Thread.pause().then( () ->
      Thread.pauseStats().then( () ->
        deferred.resolve()
      )
    )
    deferred.promise

  # Begins a thread, resetting stats
  @restart = () ->
    Thread = @thread()
    console.log('Begins a thread, resetting stats', Thread)
    deferred = Q.defer()
    Thread.pauseStats().then( () ->
      Thread.resetStats().then( () ->
        Thread.startStats().then( () ->
          Thread.start().then( () ->
            deferred.resolve()
          )
        )
      )
    )
    deferred.promise

  # Stops a thread, removing queues and resetting stats
  @stop = () ->
    Thread = @thread()
    Queue = @queue()
    console.log('Stops a thread, removing queues and resetting stats', Thread. Queue)
    deferred = Q.defer()
    Thread.stop().then( () ->
      Thread.pauseStats().then( () ->
        Thread.resetStats().then( () ->
          Queue.removeQueue().then( () ->
            Thread.destroy( () ->
              deferred.resolve()
            )
          )
        )
      )
    )
    deferred.promise

  # Empties any queued work
  @flush = () ->
    Thread = @thread()
    console.log('Processes the task Queue without doing any work, resetting stats when finished', Thread)
    Thread.pause().then( () ->
      Thread.pauseStats().then( () ->
        Thread.flush().then( () ->
          Thread.resetStats().then( () ->
            Thread.start().then( () ->
              deferred.resolve()
            )
          )
        )
      )
    )
    Thread.flush()

  # Returns stats for the current Thread
  @stats = () ->
    Thread = @thread()
    console.log('Returns stats for the current Thread', Thread)
    deferred = Q.defer()
    Thread.stats().then( (response) ->
      deferred.resolve(response)
    )
    deferred.promise

  # Toggles debug mode for Thread and Queue
  @debug = () ->
    Queue = @queue()
    Thread = @thread()
    console.log('Returns stats for the current Thread', Thread, Queue)
    that = @
    deferred = Q.defer()
    setTimeout(() ->
      that.currentDebug ?= false
      that.currentDebug = !that.currentDebug
      Thread.debug(that.currentDebug)
      Queue.debug(that.currentDebug)
      deferred.resolve()
    )
    deferred.promise

  @task = (message) ->
    deferred = Q.defer()
    Queue = @queue
    Queue.sendMessage(message).then( () ->
      deferred.resolve()
    )
    deferred.promise

  Job = ((work) ->

    # The work that will be done
    @_work = work

    # A unique ID for the work that will be done
    @_id = null

    # A method to add work
    @task = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.task().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise

    # A method to start work
    @start = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.start().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise

    # A method to pause work
    @pause = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.pause().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise

    # A method to start work and reset stats
    @restart = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.restart().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise
    # A method to stop work and destroy any queues
    @stop = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.stop().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise
    # A method to purge any pending work
    @flush = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.flush().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise
    # A method to see how much work an agent is doing
    @stats = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.stats().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise
    # A method to toggle the console.log debug state
    @debug = () ->
      deferred = Q.defer()
      @agent.then(() ->
        that.debug().then( () ->
          deferred.resolve()
        )
      )
      deferred.promise
  ).bind(@)

  Job.prototype = promise.constructor
  promise.constructor = new Job(fn)

  promise

# Sugar for configuring once instead of per-Agent
Agent::config = (config) ->
  @config = config

exports = Agent