fivebeans    = require('fivebeans')
rest         = require('restler')
util         = require('util')
EventEmitter = require('events').EventEmitter

RETRY_PRIORITY = 11
RETRY_DELAY    = 5 # <-- backoff is this times the number of attempts
                   #     total time is 1*5 + 2*5 + 3*5 + ...


RESERVE_WAIT_TIME = 2.5
BURY_PRIORITY = 1024
DEFAULT_TTR = 300

exports = {}

exports.buildWorker = (opts)->
    workerId      = opts.workerId      ? throw new Error('workerId is required')
    beanstalkHost = opts.beanstalkHost ? throw new Error('beanstalkHost is required')
    beanstalkPort = opts.beanstalkPort ? throw new Error('beanstalkPort is required')
    eventsTube    = opts.eventsTube    ? throw new Error('eventsTube is required')
    clientTimeout = opts.clientTimeout ? throw new Error('clientTimeout is required')
    maxRetries    = opts.maxRetries    ? throw new Error('maxRetries is required')
    logger        = opts.logger        ? throw new Error('logger is required')

    keenProjectID        = opts.keenProjectID        ? throw new Error('keenProjectID is required')
    keenAPIKey           = opts.keenAPIKey           ? throw new Error('keenAPIKey is required')
    mixpanelProjectToken = opts.mixpanelProjectToken ? throw new Error('mixpanelProjectToken is required')
    slackWebAPIToken     = opts.slackWebAPIToken     ? throw new Error('slackWebAPIToken is required')
    slackUsername        = opts.slackUsername        ? throw new Error('slackUsername is required')
    slackChannel         = opts.slackChannel         ? throw new Error('slackChannel is required')
    slackIconURL         = opts.slackIconURL         ? throw new Error('slackIconURL is required')

    self = new EventEmitter()

    clientConnected = ()->
        logger.silly("worker #{workerId} connected")
        connected = true
        watchTubes()
        return

    clientErrored = (err)->
        logger.warn("client error", {name: 'client.error', error: err, workerId: workerId})
        self.emit('error.client', err)
        if not shutdownRequested
            shutdownClient()
        return

    clientClosed = ()->
        connected = false
        self.running = false
        self.busy = false
        logger.silly("client closed", {workerId: workerId})
        self.emit('closed', workerId)
        return

    shutdownClient = ()->
        client.end()
        writeClient.end()
        connected = false
        self.running = false
        self.busy = false
        return

    watchTubes = ()->
        client.watch eventsTube, (err, numwatched)->
            if err
                logger.warn("error watching eventsTube", {name: 'eventsTubeWatch.error', error: err, eventsTube: eventsTube, workerId: workerId})
                shutdownClient()
                return
            self.emit('next')
            return

    runNextJob = ()->
        if shutdownRequested
            shutdownClient()
            return

        # logger.silly("runNextJob", {workerId: workerId})
        client.reserve_with_timeout RESERVE_WAIT_TIME, (err, jobId, payload)->
            if err
                if err == 'TIMED_OUT'
                    self.emit('loop')
                    self.emit('next')
                    return

                logger.warn("client error", {name: 'client.error', error: err, workerId: workerId})
                shutdownClient()
                return
            try

                logger.silly("running job #{jobId}")
                jobData = JSON.parse(payload)
                # console.log "jobData is "+JSON.stringify(jobData)
                self.busy = true
                processJob jobId, jobData, (completed)->
                    client.destroy jobId, (err)->
                        self.busy = false
                        if err
                            logger.warn("destroy error", {name: 'error.destroy', error: err, jobId: jobId, workerId: workerId})
                            shutdownClient()
                            return

                        self.emit('next')
                        return
                    return



            catch e
                self.busy = false
                logger.warn("error.uncaught "+e, {name: 'error.uncaught', error: ''+e, jobId: jobId, workerId: workerId})
                self.emit('error.job', e)
                shutdownClient()
            return



    processJob = (jobId, jobData, callback)->
        success = false

        # handle bad job data
        if not jobData.meta?.attempt?
            logger.warn("error.badJob", {name: 'error.badJob', jobData: JSON.stringify(jobData), jobId: jobId, workerId: workerId})
            callback(true)
            return

        try
            # call the callback
            logger.info("begin processing job", {name: 'job.begin', jobId: jobId, notificationId: jobData.meta.id, attempt: jobData.meta.attempt, maxRetries: maxRetries, jobType: jobData.meta.jobType, })


            if jobData.meta.jobType == 'mixpanel'
                jobHandler = processMixpanelJob
            else if jobData.meta.jobType == 'keen'
                jobHandler = processKeenStatsJob
            else if jobData.meta.jobType == 'slack'
                jobHandler = processSlackJob
            else
                finishJob(false, 'Unknown jobtype: '+jobData.meta.jobType, jobData, jobId, callback)
                return

            jobHandler(jobData, jobId, callback)

        catch err
            console.error(err)
            logger.error("Unexpected error", {name: 'job.errorUnexpected', jobId: jobId, notificationId: jobData.meta?.id?, error: err.message, href: href, })
            finishJob(false, "Unexpected error: "+err, jobData, jobId, callback)
            return

        return

    finishJob = (success, errString, jobData, jobId, callback)->
        # if done
        #   then push the job back to the beanstalk notification_result queue with the new state
        finished = false
        if success
            finished = true
        else
            # error
            if jobData.meta.attempt >= maxRetries
                logger.warn("Job giving up after attempt #{jobData.meta.attempt}", {name: 'job.failed', jobId: jobId, notificationId: jobData.meta.id, attempts: jobData.meta.attempt, })
                finished = true
            else
                logger.debug("Retrying job after error", {name: 'job.retry', jobId: jobId, notificationId: jobData.meta.id, attempts: jobData.meta.attempt, error: errString, })


        if finished
            _logData = {name: 'job.finished', jobId: jobId, notificationId: jobData.meta.id, href: jobData.meta.endpoint, totalAttempts: jobData.meta.attempt, success: success, }
            _logData.error = errString if errString
            logger.info("Job finished", _logData)

            # just do the callback to finish the job
            callback(true)
        else
            # retry
            insertJobIntoBeanstalk eventsTube, jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, (loadSuccess)->
                callback(loadSuccess)
        return


    # ------------------------------------------------------------------------
    # beanstalk write client

    writeClientConnected = ()->
        writeClientReady = true
        return

    writeClientErrored = (err)->
        logger.warn("writeClient error", {name: 'error.writeClient', error: err, workerId: workerId})
        shutdownClient()
        return

    writeClientClosed = ()->
        writeClientReady = false
        logger.silly("writeClient closed", {workerId: workerId})
        if startingUpPhase
            # since we are still starting up, the main client won't send a closed event
            #   so we have to
            self.emit('closed', workerId)
        return

    insertJobIntoBeanstalk = (queue, data, retry_priority, retry_delay, callback, attempts)->
        if not attempts?
            attempts = 0

        writeClient.use queue, (err, tubename)->
            if err
                logger.warn("error selecting queue", {name: 'error.writeClientWatch', error: err, queue: queue, workerId: workerId})
                callback(false)
                return

            if tubename != queue
                ++attempts
                if attempts > 3
                    logger.warn("queue mismatch", {name: 'error.fatalQueueMismatch', attempts: attempts, tubename: tubename, queue: queue, workerId: workerId})
                else
                    logger.warn("queue mismatch", {name: 'error.queueMismatch', attempts: attempts, tubename: tubename, queue: queue, workerId: workerId})
                    # retry
                    setTimeout ()->
                        insertJobIntoBeanstalk(queue, data, retry_priority, retry_delay, callback, attempts)
                    , attempts * 125
                return




            writeClient.put retry_priority, retry_delay, DEFAULT_TTR, JSON.stringify(data), (err, jobId)->
                if err
                    logger.warn("error loading job", {name: 'loadJob.failed', queue: queue})
                    callback(false)
                    return

                callback(true)
                return

        return

    processKeenStatsJob = (jobData, jobId, callback)->
        if not keenAPIKey
            finishJob(true, 'No keenAPIKey defined', jobData, jobId, callback)
            return
        if not keenProjectID
            finishJob(true, 'No keenProjectID defined', jobData, jobId, callback)
            return

        href = "https://api.keen.io/3.0/projects/#{keenProjectID}/events/#{jobData.meta.collection}?api_key=#{keenAPIKey}"
        requestData = JSON.stringify(jobData.data)
        processRestlerJob(jobData, jobId, href, true, requestData, callback)

    processMixpanelJob = (jobData, jobId, callback)->
        if not mixpanelProjectToken
            finishJob(true, 'No mixpanelProjectToken defined', jobData, jobId, callback)
            return

        mixpanelEvent = {
            event: jobData.meta.collection
            properties: jobData.data
        }
        mixpanelEvent.properties.token = mixpanelProjectToken


        href = "http://api.mixpanel.com/track/"
        requestData = {
            data: new Buffer(JSON.stringify(mixpanelEvent)).toString('base64')
        }
        processRestlerJob(jobData, jobId, href, false, requestData, callback)

    processSlackJob = (jobData, job, callback)->
        if not slackWebAPIToken
            finishJob(true, 'No slackWebAPIToken defined', jobData, job, callback)
            return

        requestData = {
            token: slackWebAPIToken
            username: slackUsername
            channel: slackChannel
            icon_url: slackIconURL
            text: ''
        }
        for k, v of jobData.data
            requestData[k] = v
        logger.silly("requestData", requestData)


        href = "https://slack.com/api/chat.postMessage"
        processRestlerJob(jobData, job, href, false, requestData, callback)


    processRestlerJob = (jobData, jobId, href, isPost, requestData, callback)->
        success = false

        # call the callback
        if not jobData.meta?.attempt?
            jobData.meta.attempt = 0
        jobData.meta.attempt = jobData.meta.attempt + 1

        # console.log("requestData: ",requestData)

        try
            logger.silly("begin processRestlerJob #{jobId} (attempt #{jobData.meta.attempt} of #{maxRetries}, href #{href})")

            callParams = {
                headers: {'User-Agent': 'Tokenly Event Proxy'}
                timeout: clientTimeout
            }

            if isPost
                callParams.headers['Content-Type'] = 'application/json'
                callFn = rest.post
                callParams.data = requestData
            else
                callFn = rest.get
                callParams.query = requestData

            callFn(href, callParams).on 'complete', (data, response)->
                # console.log "data",data
                # console.log "response",response
                msg = ''
                if response
                    logger.silly("received HTTP response: "+response?.statusCode?.toString(), {statusCode: response?.statusCode?.toString()})
                else
                    logger.warn("received no HTTP response")
                if response? and response.statusCode.toString().charAt(0) == '2'
                    success = true
                else
                    success = false
                    if response?
                        msg = "ERROR: received HTTP response with code "+response.statusCode
                        logger.warn("HTTP error", {jobId: jobId, statusCode: response.statusCode})
                    else
                        if data instanceof Error
                            msg = ""+data
                        else
                            msg = "ERROR: no HTTP response received"

                # if DEBUG then console.log "[#{new Date().toString()}] #{jobId} finish success=#{success}"
                finishJob(success, msg, jobData, jobId, callback)
                return

            .on 'timeout', (e)->
                logger.warn("Job timeout", {jobId: jobId})

                # 'complete' will not be called on a timeout failure
                finishJob(false, "Timeout: "+e, jobData, jobId, callback)
                
                return

            .on 'error', (e)->
                # 'complete' will be called after this
                return

        catch err
            logger.silly("[#{new Date().toString()}] Caught ERROR:", err)
            logger.silly(err.stack)

            finishJob(false, "Unexpected error: "+err, jobData, jobId, callback)
            return

        return

    # ------------------------------------------------------------------------


    client = null
    writeClient = null
    writeClientReady = false
    connected = false
    shutdownRequested = false
    self.running = false
    self.busy = false
    startingUpPhase = false
    self.on 'next', runNextJob

    initClients = ()->
        client = new fivebeans.client(beanstalkHost, beanstalkPort)
        client.on 'connect', clientConnected
        client.on 'error', clientErrored
        client.on 'close', clientClosed

        writeClient = new fivebeans.client(beanstalkHost, beanstalkPort)
        writeClientReady = false
        writeClient.on 'connect', writeClientConnected
        writeClient.on 'connect', ()->
            # connect the read client AFTER the write client
            startingUpPhase = false
            client.connect()
            return
        writeClient.on 'error', writeClientErrored
        writeClient.on 'close', writeClientClosed
        return

    self.getId = ()->
        return workerId
    
    self.run = ()->
        logger.silly("launching worker #{workerId}")
        client = null
        writeClient = null
        shutdownRequested = false
        self.running = true
        startingUpPhase = true
        initClients()
        writeClient.connect()
        return

    self.stop = ()->
        shutdownRequested = true
        return
    
    return self

module.exports = exports