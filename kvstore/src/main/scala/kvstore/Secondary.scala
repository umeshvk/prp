package kvstore

import akka.event.LoggingReceive
import kvstore.Persistence.{Persisted, Persist}
import kvstore.Replicator.{SnapshotAck, Snapshot}
import scala.concurrent.duration._
import scala.language.postfixOps
/**
 * Created by umesh on 5/31/15.
 */
trait Secondary {

  this: Replica =>

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher


  /* TODO Behavior for the replica role. */
  val secondaryReplica: Receive = LoggingReceive {

    //Message from Client
    case Get(key, id) => {
      val value: Option[String] = cache get key
      sender ! GetResult(key, value, id)
    }

    //Message from Replicator in Primary Replica
    case Snapshot(key, valueOption, seq) => {
      if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      }
      else if (seq == expectedSeq) {
        valueOption match {

          //Insert this value from cache and persistor
          case Some(value) => {
            //Add in memory
            cache += key -> value
            //Add to disk
            val cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistor, Persist(key, Some(value), seq))
            secondaryPersistingAcks +=
              seq -> (sender, /** Replicator Ref **/
                cancellable)
          }

          //Remove this value from cache and persistor
          case None => {
            //Remove from memory
            cache -= key
            //Add to disk
            val cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistor, Persist(key, None, seq))
            secondaryPersistingAcks += seq -> (sender,  /** Replicator Ref **/
              cancellable)
          }
        }
        expectedSeq += 1
      } else {
        log.info("Do nothing seq= {} and expectedSeq = {}", seq, expectedSeq)
      }
    }
    //Message from Persistor
    case Persisted(key, id) => {
      secondaryPersistingAcks get id match {
        case Some((replicator, cancellable)) => {
          cancellable.cancel
          secondaryPersistingAcks -= id
          replicator ! SnapshotAck(key, id)
        }
        case None =>
      }
    }
  }
}
