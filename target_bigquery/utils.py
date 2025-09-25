import json
import os
import sys

import singer


logger = singer.get_logger()


def emit_state(state):
    """
    Given a state, writes the state to a state file (e.g., state.json.tmp)

    :param state, State: state with bookmarks dictionary
    """
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

        if os.environ.get("TARGET_BIGQUERY_STATE_FILE", None):
            fn = os.environ.get("TARGET_BIGQUERY_STATE_FILE", None)
            with open(fn, "a") as f:
                f.write("{}\n".format(line))

