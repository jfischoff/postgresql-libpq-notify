{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes, RecordWildCards #-}
{-|
Module      :  Database.PostgreSQL.LibPQ.Notify
Copyright   :  (c) 2020 Jonathan Fischoff
               (c) 2016 Moritz Kiefer
               (c) 2011-2015 Leon P Smith
               (c) 2012 Joey Adams
License     :  BSD3
Maintainer  :  Moritz Kiefer <moritz.kiefer@purelyfunctional.org>
Support for receiving asynchronous notifications via PostgreSQL's
Listen/Notify mechanism.  See
<http://www.postgresql.org/docs/9.4/static/sql-notify.html> for more
information.

Note that on Windows,  @getNotification@ currently uses a polling loop
of 1 second to check for more notifications,  due to some inadequacies
in GHC's IO implementation and interface on that platform.   See GHC
issue #7353 for more information.  While this workaround is less than
ideal,  notifications are still better than polling the database directly.
Notifications do not create any extra work for the backend,  and are
likely cheaper on the client side as well.

<http://hackage.haskell.org/trac/ghc/ticket/7353>

PostgreSQL notifications support using the same connection for sending and
recieving notifications. This pattern is particularly useful in tests and
probably not that useful otherwise.

This library relies on receiving notifications that new data is available to
read from the connection's socket. However both epoll_wait and kqueue do
not return if recvfrom is able to clear the socket receive buffer
before they verify epoll_wait/kqueue verify there is data available.

This race between recvfrom and the event processing in kqueue/epoll_wait is common
and largely unavoidable if one is using the same connection for sending and
receiving notifications on different threads simultaneously.

To support this pattern the library provides an advanced API which allows a customer
interrupt to be passed in. See 'getNotificationWithConfig' for more details.

-}

module Database.PostgreSQL.LibPQ.Notify
     ( getNotification
     -- ** Advanced API
     , getNotificationWithConfig
     , defaultConfig
     , Config (..)
     ) where

import           Control.Exception (try, throwIO)
import qualified Database.PostgreSQL.LibPQ as PQ
import           GHC.IO.Exception (IOException(..),IOErrorType(ResourceVanished))
#if defined(mingw32_HOST_OS)
import           Control.Concurrent (threadDelay)
#else
import           Control.Concurrent
import           Control.Concurrent.STM(atomically)
#endif
import           Data.Function(fix)
import           Data.Bifunctor(first)
import           Control.Concurrent.Async (race)


data Config = Config
  { interrupt              :: Maybe (IO ())
  -- ^ Custom interrupt
  , interrupted            :: IO ()
  -- ^ Event hook called if the 'interrupt' is set and returns
  --   before the 'threadWaitReadSTM' action returns.
  , threadWaitReadReturned :: IO ()
  -- ^ Event hook called if 'threadWaitReadSTM' action returns before the
  --   'interrupt' returns action (if one is set).
  , startLoop              :: IO ()
  -- ^ Called each time 'getNotificationWithConfig' loops to look for another notification
  , beforeWait             :: IO ()
  -- ^ Event hook called before the thread will
  -- wait on new data or 'interrupt' returning.
#if defined(mingw32_HOST_OS)
  , retryDelay             :: Int
  -- ^ How long to wait in microseconds before retrying on Windows.
#endif
  }

defaultConfig :: Config
defaultConfig = Config
  { interrupt              = Nothing
  , interrupted            = pure ()
  , threadWaitReadReturned = pure ()
  , startLoop              = pure ()
  , beforeWait             = pure ()
#if defined(mingw32_HOST_OS)
  , retryDelay             = 100000
#endif
  }

data RetryOrReturn = Retry (IO (Either () ())) | Return PQ.Notify

funcName :: String
funcName = "Hasql.Notification.getNotification"

setLoc :: IOError -> IOError
setLoc err = err {ioe_location = funcName}

fdError :: IOError
fdError =
  IOError { ioe_handle = Nothing
          , ioe_type = ResourceVanished
          , ioe_location = funcName
          , ioe_description =
              "failed to fetch file descriptor (did the connection time out?)"
          , ioe_errno = Nothing
          , ioe_filename = Nothing
          }

{-|
Returns a single notification.  If no notifications are
available, 'getNotificationWithConfig' blocks until one arrives.
Unlike 'getNotification', 'getNotificationWithConfig' takes in an
additional 'Config' parameter which provides custom 'interrupt' and
various event hooks for operational insight.

Using a custom 'interrupt' is necessary if one would like to call
'getNotificationWithConfig' on one thread and @NOTIFY@ on another
thread using the same connection.

To support this behavior one must cause 'interrupt' to return after the
call to @NOTIFY@ checks it's result from the server.

See the test file of this package for an example of how to use a custom
'interrupt'.

Note that PostgreSQL does not
deliver notifications while a connection is inside a transaction.
-}
getNotificationWithConfig
  :: Config
  -- ^
  -> (forall a. c -> (PQ.Connection -> IO a) -> IO a)
  -- ^ This is a way to get a connection from a 'c'.
  --   A concrete example would be if 'c' is 'MVar PQ.Connection'
  --   and then this function would be 'withMVar'
  -> c
  -- ^ A type that can used to provide a connection when
  --   used with the former argument. Typically a concurrency
  --   primitive like 'MVar PQ.Connection'
  -> IO (Either IOError PQ.Notify)
getNotificationWithConfig Config {..} withConnection conn = fmap (first setLoc) $ try $ fix $ \next -> do
    -- We try to get the notification or register a file descriptor callback
    -- while holding the lock. We then give up the lock to wait.
    -- That is why code is broken up into these two sections.
    startLoop
    e <- withConnection conn $ \c -> PQ.consumeInput c >> PQ.notifies c >>= \case
      -- We found a notification just return it
      Just x -> pure $ Return x
      -- There wasn't a notification so we need to register to wait on the file handle
      Nothing -> PQ.socket c >>= \case
        -- This is an odd error
        Nothing -> throwIO fdError
        -- Typical case. Register to wait on more data.
        Just fd -> do
#if defined(mingw32_HOST_OS)
          let fileNotification = threadDelay retryDelay
#else
          -- We use threadWaitReadSTM instead of threadWaitRead to ensure the
          -- read callback is register while the look is held.
          action <-fst <$> threadWaitReadSTM fd
          -- Either wait for the file notification or race against the notification
          -- with the custom interrupt event if one is provided
          let fileNotification = atomically action
#endif
          pure $ Retry $ maybe (pure <$> fileNotification) (fileNotification `race`) interrupt

    case e of
      Retry raceResult -> do
        beforeWait
        -- Wait on either threadWaitReadSTM action or the custom interrupt
        either (const interrupted) (const threadWaitReadReturned) =<< raceResult
        next
      Return x -> pure x

{-|
Returns a single notification.  If no notifications are
available, 'getNotification' blocks until one arrives.

If 'getNotification' is called and afterwards on a different thread
@NOTIFY@ is called using the same connection, 'getNotification' can
block even if a notification is sent.

To support this behavior one must use 'getNotificationWithConfig' instead.

Note that PostgreSQL does not
deliver notifications while a connection is inside a transaction.
-}
getNotification
  :: (forall a. c -> (PQ.Connection -> IO a) -> IO a)
  -- ^ This is a way to get a connection from a 'c'.
  --   A concrete example would be if 'c' is 'MVar PQ.Connection'
  --   and then this function would be 'withMVar'
  -> c
  -- ^ A type that can used to provide a connection when
  --   used with the former argument. Typically a concurrency
  --   primitive like 'MVar PQ.Connection'
  -> IO (Either IOError PQ.Notify)
getNotification = getNotificationWithConfig defaultConfig
