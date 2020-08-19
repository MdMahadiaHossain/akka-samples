package sample.persistence.multidc

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.{ReplicatedEntity, ReplicatedEntityProvider, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.typed.{ReplicaId, ReplicationId}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplicatedEventSourcing}

object ThumbsUpCounter {

  // sent over sharding
  sealed trait Command extends CborSerializer {
    def resourceId: String
  }
  final case class GiveThumbsUp(resourceId: String, userId: String, replyTo: ActorRef[Long]) extends Command
  final case class GetCount(resourceId: String, replyTo: ActorRef[Long]) extends Command
  final case class GetUsers(resourceId: String, replyTo: ActorRef[State]) extends Command

  // saved to DB
  sealed trait Event extends CborSerializer
  final case class GaveThumbsUp(userId: String) extends Event

  // saved to DB
  final case class State(users: Set[String]) extends CborSerializer {
    def add(userId: String): State = copy(users + userId)
  }

  // In this sample one replica per DC
  val Replicas = Set(ReplicaId("eu-west"), ReplicaId("eu-central"))

  val Provider: ReplicatedEntityProvider[Command, ShardingEnvelope[Command]] = ReplicatedEntityProvider[Command, ShardingEnvelope[Command]](
    "counter", Replicas
  ) { (entityTypeKey, replicaId) =>
    ReplicatedEntity(replicaId, Entity(entityTypeKey) { entityContext =>
      val replicationId = ReplicationId.fromString(entityContext.entityId)
      ThumbsUpCounter(replicationId)
    }.withDataCenter(replicaId.id))
  }


  // we use a shared journal as cassandra typically spans DCs rather than a DB per replica
  def apply(replicationId: ReplicationId): Behavior[Command] =
    Behaviors.setup { ctx =>
      ReplicatedEventSourcing.withSharedJournal(replicationId, Replicas, CassandraReadJournal.Identifier) { replicationContext =>
        EventSourcedBehavior[Command, Event, State](
          replicationId.persistenceId,
          State(Set.empty),
          (state, cmd) => cmd match {
            case GiveThumbsUp(_, userId, replyTo) =>
              Effect.persist(GaveThumbsUp(userId)).thenRun { state2 =>
                ctx.log.info("Thumbs-up by {}, total count {}", userId, state2.users.size)
                replyTo ! state2.users.size
              }
            case GetCount(_, replyTo) =>
              replyTo ! state.users.size
              Effect.none
            case GetUsers(_, replyTo) =>
              replyTo ! state
              Effect.none
          },
          (state, event) => event match {
            case GaveThumbsUp(userId) => state.add(userId)
          }
        )
      }

    }
}
