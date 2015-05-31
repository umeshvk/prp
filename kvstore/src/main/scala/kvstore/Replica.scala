package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import akka.event.LoggingReceive
import collection.mutable.{ HashMap, MultiMap }
import collection.immutable.Set
import scala.language.postfixOps



object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}


//Primary Send Message List: ( To Client: OperationAck(id), OperationFailed(id), GetResult(key, valueOption, id) )
//contd//
//Primary Receive Message List: (From Client : INSERT(key, value, id), REMOVE(key, id), Get(key, id))
//Secondary Send Message List: ( To Client: GetResult(key, valueOption, id) )
//Secondary Receive Message List: (From Client :  Get(key, id))
class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging{
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var cache = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaryReplicaToReplicatorMap = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var secondaryReplicatorRefSet = Set.empty[ActorRef]


  val persistor = context.system.actorOf(persistenceProps)
  context.watch(persistor)

  var counter = 0

  var primaryPersistingAcks = Map.empty[Long, (ActorRef, Cancellable)]
  //Key is id of the operation: insert, remove, get
  //Value is (ClientRef, NumberOfSecondaryRefs at point of replication request to replicator)
  var replicationAcks = Map.empty[Long, (ActorRef, Long)]
  //Key is replicator actor ref
  var replicatorAcks = new HashMap[ActorRef, collection.mutable.Set[Long]] with MultiMap[ActorRef, Long]

  arbiter ! Join


  var expectedSeq = 0L
  var secondaryPersistingAcks = Map.empty[Long, (ActorRef, Cancellable)]


  def receive = LoggingReceive {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(secondaryReplica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {
    case Insert(key, value, id) =>
      insert(key, value, id)
    case Remove(key, id) =>
      remove(key, id)
    case Get(key, id) =>
      get(key, id)
    //From Arbiter
    case Replicas(replicas) =>
      arbiterInforms(replicas)
    //From Replicator
    case Replicated(key, id) =>
      replicatorReplicated(key, id)
    case Persisted(key, id) =>
      persistConfirm(key, id)
    case _ => log.info("Got message in primary replica")

  }

  def insert(key: String, value: String, id: Long) : Unit = {
    cache += (key -> value)

    if (!secondaryReplicatorRefSet.isEmpty) {
      replicationAcks += (id -> (sender, secondaryReplicatorRefSet.size))
      secondaryReplicatorRefSet foreach { replicatorRef =>
        replicatorAcks.addBinding(replicatorRef, id)
        replicatorRef ! Replicate(key, Some(value), id)
      }
    }

    val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistor, Persist(key, Some(value), id))
    primaryPersistingAcks += (id -> (sender /* ClientRef */, cancellable))

    context.system.scheduler.scheduleOnce(1 second) {
      primaryPersistingAcks get id match {
        case Some((s, cancellable)) => {
          cancellable.cancel
          primaryPersistingAcks -= id
          s ! OperationFailed(id)
        }
        case None => {
          replicationAcks get id match {
            case Some((s, c)) => {
              replicationAcks -= id
              s ! OperationFailed(id)
            }
            case None =>
          }
        }
      }
    }
  }

  def remove(key: String, id: Long) : Unit = {
    cache -= key

    if (!secondaryReplicatorRefSet.isEmpty) {
      replicationAcks += id -> (sender, secondaryReplicatorRefSet.size)
      secondaryReplicatorRefSet foreach { replicatorRef =>
        replicatorAcks.addBinding(replicatorRef, id)
        replicatorRef ! Replicate(key, None, id)
      }
    }

    val cancellable : Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistor, Persist(key, None, id))
    primaryPersistingAcks += id -> (sender /* Client Ref */, cancellable)

    context.system.scheduler.scheduleOnce(1 second) {
      primaryPersistingAcks get id match {
        case Some((clientRef, cancellable)) => {
          cancellable.cancel
          primaryPersistingAcks -= id
          clientRef ! OperationFailed(id)
        }
        case None => {
          replicationAcks get id match {
            case Some((s, c)) => {
              replicationAcks -= id
              s ! OperationFailed(id)
            }
            case None =>
          }
        }
      }
    }
  }

  def get(key: String, id: Long) : Unit =  {
    val value: Option[String] = cache get key
    sender ! GetResult(key, value, id)
  }

  def arbiterInforms(replicas: Set[ActorRef]) : Unit = {

    val secondaryActorRefs = replicas - self
    assert(secondaryActorRefs.size == (replicas.size -1))
    val joinedSet = secondaryActorRefs -- secondaryReplicaToReplicatorMap.keySet
    log.info("Numbers of joiners: {}", joinedSet.size)
    val leftSet =  secondaryReplicaToReplicatorMap.keySet -- secondaryActorRefs
    log.info("Numbers of leavers: {}", leftSet.size)

    joinedSet foreach {
      newSecondaryReplicaRef =>
        val secondaryReplicatorRef = context.system.actorOf(Replicator.props(newSecondaryReplicaRef))
        context.watch(secondaryReplicatorRef)
        secondaryReplicaToReplicatorMap += newSecondaryReplicaRef -> secondaryReplicatorRef
        secondaryReplicatorRefSet += secondaryReplicatorRef
        cache foreach { kvTuple =>
          secondaryReplicatorRef ! Replicate(kvTuple._1, Some(kvTuple._2), counter)
          counter += 1
        }
    }

    leftSet foreach { leftActorRef =>
      secondaryReplicaToReplicatorMap get leftActorRef match {
        case Some(replicatorRef) => {
          context.stop(replicatorRef)
          secondaryReplicaToReplicatorMap -= leftActorRef
          secondaryReplicatorRefSet -= replicatorRef

          replicatorAcks get replicatorRef match {
            case Some(outstandingAcks) => {
              outstandingAcks foreach { a =>
                self ! Replicated("", a)
              }
              replicatorAcks -= replicatorRef
            }
            case None =>
          }
        }
        case None =>
      }
    }
  }

  def replicatorReplicated(key: String, id: Long) :Unit = {
    replicatorAcks get sender match {
      case Some(boundIdSet) => {
        boundIdSet -= id
      }
      case None =>
    }
    replicationAcks get id match {
      case Some((clientRef, replicatorRefCount)) => {
        val newValue = replicatorRefCount - 1
        if (newValue == 0) {
          replicationAcks -= id
          if (!(primaryPersistingAcks contains id)) {
            clientRef ! OperationAck(id)
          }
        } else {
          replicationAcks += id -> (clientRef, newValue)
        }
      }
      case None =>
    }
  }

  def persistConfirm(key: String, id: Long) : Unit = {
    primaryPersistingAcks get id match {
      case Some((s, cancellable)) => {
        cancellable.cancel
        primaryPersistingAcks -= id
        if (!(replicationAcks contains id)) {
          s ! OperationAck(id)
        }
      }
      case None =>
    }
  }

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
