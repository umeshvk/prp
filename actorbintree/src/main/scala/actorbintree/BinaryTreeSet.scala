/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import akka.pattern.{ ask, pipe }

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal


  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
      case Insert(requester: ActorRef, id: Int, elem: Int) =>
        root ! Insert(requester, id, elem)
      case Contains(requester: ActorRef, id: Int, elem: Int) =>
        root ! Contains(requester, id, elem)
      case Remove(requester: ActorRef, id: Int, elem: Int) =>
        root ! Remove(requester, id, elem)
      case GC =>
        println("GC requested")
        val newRoot = createRoot
        context.become(garbageCollecting(newRoot))

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case operation: Operation =>
      pendingQueue enqueue operation
    case GC =>
      root ?  CopyTo(newRoot)
      root = newRoot
      context.unbecome()


}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, elemIn: Int) =>
      if(elemIn <= elem) {
        val leftTreeOp = subtrees get Left
        leftTreeOp match {
          case Some(ref) =>
            ref ! Insert(requester: ActorRef, id: Int, elemIn: Int)
          case None =>
            val leftTree = context.actorOf(BinaryTreeNode.props(elemIn, initiallyRemoved = false))
            val rightTree = subtrees get Right
            rightTree match {
              case None => subtrees = Map(Left -> leftTree)
              case Some(ref) => subtrees = Map(Left -> leftTree, Right -> ref)
            }
            requester ! OperationFinished(id=id)
        }
      } else {
        val rightTreeOp = subtrees get Right
        rightTreeOp match {
          case Some(ref) =>
            ref ! Insert(requester, id, elemIn)
          case None =>
            val rightTree = context.actorOf(BinaryTreeNode.props(elemIn, initiallyRemoved = false))
            val leftTree = subtrees get Left
            leftTree match {
              case None => subtrees = Map(Right -> rightTree)
              case Some(ref) => subtrees = Map(Left -> ref, Right -> rightTree)
            }
            requester ! OperationFinished(id=id)
        }
      }
    case Contains(requester: ActorRef, id: Int, elemIn: Int) =>
      if(elemIn == elem) {
        requester ! ContainsResult(id=id, true)
      } else {
        var found = false
        val leftTreeOp = subtrees get Left
        leftTreeOp match {
          case Some(ref) =>
            val x = ref ? Contains(requester, id, elemIn)
            if (x == ContainsResult(id = id, true)) found = true
          case _ =>
        }
        val rightTreeOp = subtrees get Right
        rightTreeOp match {
          case Some(ref) =>
            val x = ref ? Contains(requester, id, elemIn)
            if (x == ContainsResult(id = id, true)) found = true
          case _ =>
        }
        if (found) ContainsResult(id = id, true) else ContainsResult(id = id, false)
      }
    case Remove(requester: ActorRef, id: Int, elemIn: Int) =>
      if(elemIn == elem) {
        removed = true
      } else {
        val leftTreeOp = subtrees get Left
        leftTreeOp match {
          case Some(ref) =>
            val x = ref ! Remove(requester, id, elemIn)
          case _ =>
        }
        val rightTreeOp = subtrees get Right
        rightTreeOp match {
          case Some(ref) =>
            val x = ref ! Remove(requester, id, elemIn)
          case _ =>
        }
      }

      requester ! OperationFinished(id=id)

    case CopyTo(treeNode: ActorRef) =>
      treeNode ? Insert(treeNode, 1, elem)
      val leftTreeOp = subtrees get Left
      leftTreeOp match {
        case Some(ref) =>
          val x = ref ?  CopyTo(treeNode)
        case _ =>
      }
      val rightTreeOp = subtrees get Right
      rightTreeOp match {
        case Some(ref) =>
          val x = ref ?  CopyTo(treeNode)
        case _ =>
      }

  }



  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case _ =>
    //xxx
  }
}


}
