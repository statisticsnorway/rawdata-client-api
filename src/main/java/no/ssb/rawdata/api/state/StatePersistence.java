package no.ssb.rawdata.api.state;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;

import java.io.Closeable;
import java.util.Set;

public interface StatePersistence extends Closeable {

    Single<Boolean> trackCompletedPositions(String namespace, Set<String> completedPositions);

    Maybe<String> getFirstPosition(String namespace);

    Maybe<String> getLastPosition(String namespace);

    Single<Boolean> setNextPosition(String namespace, String nextPosition);

    Maybe<String> getNextPosition(String namespace);

    Maybe<String> getOffsetPosition(String namespace, String fromPositon, int offset);

    Flowable<CompletedPosition> readPositions(String namespace, String fromPosition, String toPosition);

}
