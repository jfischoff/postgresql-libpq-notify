{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, RecordWildCards #-}
import           Control.Exception
import           Database.PostgreSQL.LibPQ.Notify
import           Test.Hspec
import qualified Database.Postgres.Temp as Temp
import           Control.Concurrent.Async
import           Data.IORef
import           Control.Concurrent
import           Data.Foldable
import           Control.Monad ((<=<))
import           Database.PostgreSQL.Simple.Options as Options
import qualified Database.PostgreSQL.LibPQ as PQ

main :: IO ()
main = hspec spec

aroundAll :: forall a. ((a -> IO ()) -> IO ()) -> SpecWith a -> Spec
aroundAll withFunc specWith = do
  (var, stopper, asyncer) <- runIO $
    (,,) <$> newEmptyMVar <*> newEmptyMVar <*> newIORef Nothing
  let theStart :: IO a
      theStart = do

        thread <- async $ do
          withFunc $ \x -> do
            putMVar var x
            takeMVar stopper
          pure $ error "Don't evaluate this"

        writeIORef asyncer $ Just thread

        either pure pure =<< (wait thread `race` takeMVar var)

      theStop :: a -> IO ()
      theStop _ = do
        putMVar stopper ()
        traverse_ cancel =<< readIORef asyncer

  beforeAll theStart $ afterAll theStop $ specWith

withDBConn :: Options -> (PQ.Connection -> IO a) -> IO a
withDBConn opts f =
  bracket (PQ.connectdb (Options.toConnectionString opts))
          PQ.finish
          f

withSetup :: (PQ.Connection -> IO ()) -> IO ()
withSetup f = either throwIO pure <=< Temp.withDbCache $ \dbCache ->
  Temp.withConfig (Temp.verboseConfig <> Temp.cacheConfig dbCache) $ \db -> do
    let localhostOpts = (Temp.toConnectionOptions db)
          { host = pure "127.0.0.1"
          }

    withDBConn localhostOpts f

spec :: Spec
spec = aroundAll withSetup $ do
  describe "getNotificationWithConfig" $ describe "returns a notification" $ do
    it "before" $ \conn -> do
      connVar <- newMVar conn
      let initialChannel = "channel"
          initialData = "hi!"
      _ <- withMVar connVar $ \c -> do
          _ <- PQ.exec c $ "LISTEN " <> initialChannel
          PQ.exec c ("NOTIFY " <> initialChannel <> ", '" <> initialData <>"';")

      Right PQ.Notify {..} <- getNotification withMVar connVar
      notifyRelname `shouldBe` initialChannel
      notifyExtra `shouldBe` initialData

    it "after file notifications are registered" $ \conn -> do
      connVar <- newMVar conn
      ender <- newEmptyMVar
      beforeWaiter <- newEmptyMVar
      let initialChannel = "channel"
          initialData = "hi!"
      _ <- withMVar connVar $ \c -> PQ.exec c $ "LISTEN " <> initialChannel
      _ <- forkIO $ do
             takeMVar beforeWaiter
             _ <- withMVar connVar $ \c ->  PQ.exec c ("NOTIFY " <> initialChannel <> ", '" <> initialData <>"';")
             putMVar ender ()

      let config = defaultConfig
            { interrupt  = Just $ takeMVar ender
            , beforeWait = putMVar beforeWaiter ()
            }

      Right PQ.Notify {..} <- getNotificationWithConfig config withMVar connVar
      notifyRelname `shouldBe` initialChannel
      notifyExtra `shouldBe` initialData
