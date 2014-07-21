Queue = (args) ->
  @debug = false
  @flush = false
  @batch = true
  @queueURIs = {}
  @log = args.log
  @log ?= console.log
  @prefix = args.prefix
  @prefix ?= 'pdq-'
  region = args.region
  region ?= "us-east-1"

  that = @
  AWS = require("aws-sdk")
  AWS.config?.update
    accessKeyId: args.access_key,
    secretAccessKey: args.secret,
    region: region
  @sqs = new AWS.SQS()
  ###
    @checkQueues( { complete: () ->
    if true is that.debug then that.log( "PUBLISHER: Queues checked")
    if 'function' is typeof args.callback then args.callback( null, {} )
  } )
  ###
  @

Queue::parseQueue = (args) ->
  (args.data.match /^https:\/\/sqs.([a-zA-Z0-1\-]{1,}).amazonaws.com\/(\d{1,})\/([a-zA-Z0-9_\-]{1,})$/ ).slice(1)

Queue::getQueues = (args) ->
  that = @
  if "undefined" isnt typeof @queueCache
    if "function" is typeof args.success
      if tue is that.debug then that.log "Cached queues"
      args.success @queueCache
    return @
  @sqs.listQueues {}, (err, data) ->
    if null isnt err
      if true is that.debug then that.log "SQS: Error", err
      args.error err  if "function" is typeof args.error
    else
      queues = data?.QueueUrls
      if true is that.debug then that.log "SQS: Success", queues, typeof args.success
      if "function" is typeof args.success
        that.queueCache = queues
        args.success queues

Queue::createQueue = ( args ) ->
  name = [ @queuePrefix(), args.QueueName ].join('')
  attributes = args.Attributes
  that = @
  @sqs.createQueue { QueueName: name, Attributes: attributes }, (err, data) ->
    if true is that.debug then that.log("PUBLISHER: AWS Queue Created", err, data)
    if null isnt err or null is data
      if 'function' is typeof args.error
        args.error(err)
    else
      if 'function' is typeof args.success
        args.success(data)

Queue::checkQueues = ( args ) ->
  if true is @debug then @log( "PUBLISHER: Checking queues" )
  that = @
  queues = @queues()
  map = {}
  lookup = {}
  for queue in queues
    map[ queue ] = false
  count = 0
  expect = 0
  responses = []
  maybeFinish = (response) ->
    responses.push response
    if ( ++count >= expect )
      if 'function' is typeof args.complete
        args.complete(responses)
  @getQueues success: (queues) ->
    stack = []
    success = false
    if true is that.debug then that.log("PUBLISHER: Queues",queues)
    if 'undefined' is typeof queues
      queues = []
    for queue in queues
      parsed = that.parseQueue( { data: queue } )
      for own attr of map
        if [ @prefix, attr ].join('') is parsed[ 2 ]
          map[ attr ] = queue
    complete = true
    if true is that.debug then that.log( "PUBLISHER: Map", map )
    for own attribute, val of map
      if false is val then expect++
    for own attribute, val of map
      if false is val
        complete = false
        ( (attr, obj) ->
          if true is that.debug then that.log("PUBLISHER: Queue create", attr)
          that.createQueue( { QueueName: attr, Attributes: {}, success: (d) ->
            if true is that.debug then that.log("PUBLISHER: Queue created",d )
            obj[ attr ] = d;
            complete = true
            that.queueURIs = obj
            if 'function' is typeof args.success
              args.success(obj)
            maybeFinish(obj)
          , error: (err) ->
              if 'function' is typeof args.error
                args.error(err)
              maybeFinish(obj)
          } )
        )(attribute, lookup)
      else
        lookup[ attribute ] = val
    if true is complete
      if true is that.debug then that.log("PUBLISHER: Queues", lookup)
      that.queueURIs = lookup
      if 'function' is typeof args.success
        args.success(lookup)
      maybeFinish(lookup)
    if true is success
      if 'function' is typeof args.success
        args.success(stack)
      maybeFinish(lookup)


Queue::sendMessage = (args = {}) ->
  url = @queueURIs[ args.queue ]
  if !url
    if 'function' is typeof args.error
      arg.error()
    return
  message = args.message
  if 'string' isnt typeof message
    message = JSON.stringify( message )
  delay = args.delay
  that = @
  if 'undefined' is typeof delay
    delay = 0
  if true is that.debug then console.log("PUBLISHER: Sending message to queue URL",url)
  @sqs.sendMessage { QueueUrl: url, MessageBody: message, DelaySeconds: delay }, ( err, data ) ->
    if null isnt err
      if 'function' is typeof args.error
        args.error(err)
    else
      if 'function' is typeof args.success
        args.success( { request: data.ResponseMetadata.RequestId, data: { id: data.MessageId, md5: data.MD5OfMessageBody } } )

Queue::receiveMessage = (args) ->
  url = @queueURIs[ args.queue ]
  that = @
  max = args.max
  if 'undefined' is typeof max
    max = 1
  wait = args.wait
  if 'undefined' is typeof wait
    wait = 0
  timeout = args.timeout
  if 'undefined' is typeof timeout
    timeout = 0
  @sqs.receiveMessage( { QueueUrl: url, WaitTimeSeconds: wait, VisibilityTimeout: timeout, MaxNumberOfMessages: max, AttributeNames: [ 'SenderId', 'ApproximateFirstReceiveTimestamp', 'ApproximateReceiveCount', 'SentTimestamp' ] }, ( err, data ) ->
    if null isnt err
      if 'function' is typeof args.error
        args.error(err)
    else
      transformed = [];
      if data.Messages?
        for message in data.Messages
          if 'undefined' is typeof message.ReceiptHandle or 'undefined' is typeof message.Body
            if true is that.debug then that.log( 'No ReceiptHandle', message )
            continue
          else
            zattrs = {
              sender: message.Attributes.SenderId
              received: message.Attributes.ApproximateFirstReceiveTimestamp
              sent: message.Attributes.SentTimestamp
            }
            transformed.push( {
              message: message.Body
              receipt: message.ReceiptHandle
              md5: message.MD5OfBody
              attributes: zattrs
              id: message.MessageId
            } )
      if 'function' is typeof args.success and transformed.length > 0
        args.success( { request: data.ResponseMetadata.RequestId, data: transformed } )
      else if 'function' is typeof args.error and false is ( transformed.length > 0 )
        args.error( { request: data.ResponseMetadata.RequestId, data: transformed } )
  )
  return

Queue::deleteMessage = (args) ->
  url = @queueURIs[ args.queue ]
  receipt = args.receipt
  @sqs.deleteMessage( { QueueUrl: url, ReceiptHandle: receipt }, ( err, data ) ->
    if null isnt err
      if 'function' is typeof args.error
        args.error(err)
    else
      if 'function' is typeof args.success
        args.success( { request: data.ResponseMetadata.RequestId, data: null } )
  )

Queue::deleteMessages = (args) ->
  url = @queueURIs[ args.queue ]
  receipts = args.receipts
  that = @
  if 'undefined' is typeof receipts
    receipts = []
  receipt_map = []
  responses = []
  seen = 0
  expect = 0
  # Amazon wants a map
  maybeFinish = (data) ->
    responses.push { request: data.ResponseMetadata.RequestId, data: null }
    if ++seen >= expect
      if 'function' is typeof args.success
        args.success( responses )
  max = 0
  for receipt, i in receipts
    if 'undefined' is typeof receipt
      continue
    if 'undefined' is typeof receipt_map[ Math.floor( i / 10 ) ]
      receipt_map[ Math.floor( i / 10 ) ] = []
    receipt_map[ Math.floor( i / 10 ) ].push { 'Id':  i.toString(), 'ReceiptHandle': receipt }
    if Math.floor( i / 10 ) > max
      max = Math.floor( i / 10 )
  expect = max + 1
  if true is that.debug and max > 0 then that.log( 'PUBLISHER: Receipt map', receipt_map )
  for batch, i in receipt_map
    @sqs.deleteMessageBatch( { QueueUrl: url, Entries: receipt_map[ i ] }, ( err, data ) ->
      if null isnt err
        if true is that.debug then that.log("PUBLISHER: ERROR deleting messages", err)
        if 'function' is typeof args.error
          args.error(err)
      else
        maybeFinish(data)

    )


exports = Queue
