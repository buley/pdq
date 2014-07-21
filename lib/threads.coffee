Thread = (args) ->

  that = @
  @fuzzy = 5000

  Queues = require( __dirname__ + '/queues.coffee')
  @Q = require('Q')

  @debug = args.debug
  @debug ?= false

  @batch = args.batch
  @batch ?= true

  @stats = args.stats
  @stats = {}

  track = (thing, count = 1) ->
    if 'string' isnt typeof thing then console.log("BAD THING",thing)
    if true is that.debug then console.log("pdq: Track",thing,count)
    if 'undefined' is typeof that.stats[ thing ]
      that.stats[ thing ] = 0
    that.stats[ thing ] += count

  @thread = ( interval ) ->
    if true is that.debug then that.log("pdq: Receiving thread message")
    seen = 0
    expecting = 0
    aborted = false
    receipts = []

    max_interval = 300000
    min_interval = 1000
    interval_inc = 100
    timeout = 10
    max = 10
    interval ?= ( 5000 * Math.random() ) + 5000
    count = 0

    maybeFinish = (message) ->
      if Infinity is message
        if true is that.debug then that.log("pdq: Aborting thread with " + receipts.length + " receipts")
        if receipts.length > 0
          that.core.queue.deleteMessages( { queue, receipts, success: (data) ->
            track( queue, receipts.length )
            receipts = []
          } )
        else
          interval += Math.random() * interval_inc
          if interval > max_interval
            interval = max_interval
        that.queued_thread = setTimeout( ( () ->
          clearTimeout( that.queued_thread_watcher )
          that.queued_thread = null
          that.queue_thread( interval )
        ), interval * Math.random() )
        return
      if message?.receipt? then receipts.push message.receipt
      if true isnt that.batch
        if message.receipt?
          that.core.queue.deleteMessage( { queue, receipt: message.receipt, success: (data) ->
            if true is that.debug then that.log( 'pdq: Deleted thread',1)
            track(queue, 1)
          } )
      if ++seen >= expecting
        if true is that.batch
          if receipts.length > 0
            that.core.queue.deleteMessages( { queue, receipts, success: (data) ->
              track( queue, receipts.length )
              if true is that.debug then that.log( 'pdq: Deleted thread(s)', receipts.length)
              receipts = []
              interval -= Math.random() * interval_inc
              if interval < min_interval
                interval = min_interval
            } )
          else
            interval += Math.random() * interval_inc
            if interval > max_interval
              interval = max_interval
        that.queued_thread = setTimeout( ( () ->
          clearTimeout( that.queued_thread_watcher )
          that.queued_thread = null
          that.queue_thread( interval )
        ), interval * Math.random() )

    that.core.queue.receiveMessage( { queue, timeout, max, success: (data) ->
      expecting += data.data.length
      if expecting > 0
        for omessage in data.data
          ((message)->
            setTimeout( ( () ->
              if true is that.flush
                maybeFinish(message)
                return
              data = JSON.parse( message.message )
              that.processTweet( { data: data, complete: () ->
                that.processTweetEntities( { data: data, complete: () ->
                  maybeFinish(message)
                } )
              } )
            ), count++ * interval )
          )(omessage)
      else
        maybeFinish(Infinity)
    , error: (err) ->
        if true is that.debug then that.log( "pdq: ERROR receiving thread message", interval, err )
        maybeFinish(Infinity)
    } )
    if true isnt aborted
      clearTimeout( that.queued_thread_watcher )
  return {
    abort: () ->
      aborted = true
      maybeFinish(Infinity)
  }

  @queued_thread = null
  @queued_thread_watcher = null
  @queue_thread = (intvl) ->
    intvl ?= ( that.fuzzy * Math.random() ) + that.fuzzy
    that = @
    if null is that.queued_thread
      that.queued_thread = that.thread(intvl)
      that.queued_thread_watcher = setTimeout( () ->
        if null isnt that.queued_thread
          if 'function' is typeof that.queued_thread.abort
            that.queued_thread.abort()
          delete that[ 'queued_thread' ]
          that.queued_thread = null
          that.queue_thread(intvl)
      , intvl * 10 )


exports = Thread
