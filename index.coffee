# PDQ

# AWS key/secret
# do(message) -> decorated Q Promise
# notify(), error(), then(), etc. + start(), stop(), pause()

Q = require('Q')
Queues = require('lib/queues.coffee')
Threads = require('lib/threads.coffee')

PDQ = (config) ->
  # pretty dang queue

  # lazy queue initialization
  @queue = () ->
    # Determined Queue name and initialization state
    # If Queue w/thread name not yet initialized:
      # Check if queue with task name already exists (first memory cache then AWS)
      # If task queue doesn't yet exist in memory or on AWS:
        # Create Queue from thread name
      # Create a new Thread, passing it queue ARN
    # Return Queue

  # lazy thread initialization
  @thread = () ->
    thread = null
    # Determine Thread name and initialization state
    # If Thread w/thread name not yet intialized:
      # Lazy queue initialization
    # return Thread
    thread

  # Begins a thread
  @start = () ->
    Thread = @thread()
    console.log('Begins a thread, preserving any stats', Thread)
    deferred = Q.defer()
    Thread.start().then( () ->
      deferred.resolve()
    )
    deferred.promise

  # Stops a thread, preserving queues and stats
  @pause = () ->
    Thread = @thread()
    console.log('Stops a thread, preserving queues and stats', Thread)
    deferred = Q.defer()
    Thread.pause().then( () ->
      deferred.resolve()
    )
    deferred.promise

  # Begins a thread, resetting stats
  @restart = () ->
    Thread = @thread()
    console.log('Begins a thread, resetting stats', Thread)
    deferred = Q.defer()
    Thread.resetStats().then( () ->
      Thread.start().then( () ->
        deferred.resolve()
      )
    )
    deferred.promise

  # Stops a thread, removing queues and resetting stats
  @stop = () ->
    Thread = @thread()
    console.log('Stops a thread, removing queues and resetting stats', Thread)
    deferred = Q.defer()
    Thread.stop().then( () ->
      Thread.resetStats().then( () ->
        Queue.removeQueue().then( () ->
          Thread.destroy( () ->
            deferred.resolve()
          )
        )
      )
    )
    deferred.promise

  # Empties any queued work
  @flush = () ->
    Thread = @thread()
    console.log('Processes the task Queue without doing any work, resetting stats when finished', Thread)
    Thread.pause().then(
      Thread.flush().then(
        Thread.resetStats().then(
          Thread.start().then(
            deferred.resolve()
          )
        )
      )
    )
    Thread.flush()

  # Returns stats for the current Thread
  @stats = () ->
    console.log('Returns stats for the current Thread')
    Thread.stats()

  # Toggles debug mode for Thread and Queue
  @debug = () ->
    console.log('Returns stats for the current Thread')
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

  @work = (message) ->
    deferred = Q.defer()
    Queue.sendMessage(message).then( () ->
      deferred.resolve()
    )
    deferred.promise

# This method returns a Task, a decorated promsie representing a task whose work will
# be distributed to Threads through a queue
PDQ::task = (task) ->

  deferred = Q.defer()
  promise = deferred.promise

  Task = (() ->
    @work = that.work
    @start = that.start
    @pause = that.pause
    @restart = that.restart
    @stop = that.stop
    @flush = that.flush
    @stats = that.stats
    @debug = that.debug
  ).bind(@)
  Task.prototype = promise.constructor
  promise.constructor = new Task()

  @queue(Task).then((Queue) ->
    @thread(Task).then((Thread) ->
      Queue.sendMessage().then(() ->
        deferred.resolve()
      )
    )
  )
  return promise

exports = PDQ