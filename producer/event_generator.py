#解耦-这部分决定“产生什么业务数据”，而producer决定“怎么把数据发给kafka
#这部分的目的是写一个customer的完整journey
#journey包含多个event,每个长下面这样。
#但由于每个event里面许多内容如customer_id,device_type重复，可以提取出来
# Event type = what happened
# Properties = details of the occurrence

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4, uuid5
import random


GENERATOR_VERSION = "onboarding-v1"
DEFAULT_SEED = 42
CUSTOMER_START_INTERVAL_SECONDS = 60


@dataclass(frozen=True)
class SimulationSpec:
    """All inputs required to regenerate a batch in the pinned Python runtime."""

    run_id: str
    seed: int
    base_time: datetime
    customer_count: int

    def __post_init__(self):
        object.__setattr__(self, "run_id", str(UUID(self.run_id)))
        if type(self.seed) is not int or not 0 <= self.seed <= 2**63 - 1:
            raise ValueError("seed must be an integer between 0 and 2**63 - 1")
        if type(self.customer_count) is not int or self.customer_count <= 0:
            raise ValueError("customer_count must be a positive integer")
        if not isinstance(self.base_time, datetime) or self.base_time.utcoffset() is None:
            raise ValueError("base_time must be an ISO timestamp with a timezone")
        object.__setattr__(self, "base_time", self.base_time.astimezone(timezone.utc))
        # Reject an impossible timeline before creating files or publishing.
        self.base_time + timedelta(seconds=(self.customer_count - 1) * CUSTOMER_START_INTERVAL_SECONDS,
                                   minutes=20)

    @classmethod
    def create(cls, customer_count=100, *, run_id=None, seed=DEFAULT_SEED, base_time=None):
        timestamp = datetime.now(timezone.utc) if base_time is None else datetime.fromisoformat(base_time)
        return cls(str(uuid4()) if run_id is None else run_id, seed, timestamp, customer_count)

    def as_dict(self):
        return {"generator_version": GENERATOR_VERSION, "run_id": self.run_id,
                "seed": self.seed, "base_time": self.base_time.isoformat(),
                "customer_count": self.customer_count}


def create_customer_context(customer_number, *, rng=None, run_id=None):
    rng = random if rng is None else rng
    channel=rng.choice(["mobile_app","web","branch"])
    device_type=rng.choice(["ios","android","desktop"])
    if channel == "mobile_app":
        device_type = rng.choice(
            ["ios", "android"]
        )

    elif channel == "web":
        device_type = "desktop"

    else:
        device_type = "branch_terminal"
    if run_id is None:
        customer_id = f"C_{10000 + customer_number}"
        session_id = f"S_{10000 + customer_number}_{uuid4().hex[:8]}"
    else:
        namespace = UUID(run_id)
        identity = f"{GENERATOR_VERSION}/customer/{customer_number}"
        customer_id = "C_" + uuid5(namespace, identity).hex
        session_id = str(uuid5(namespace, identity + "/session/1"))
    context={"customer_id": customer_id, "channel": channel,
             "device_type": device_type, "session_id": session_id}
    if run_id is not None:
        context["run_id"] = str(UUID(run_id))
    return context


def create_event(context, event_type, event_time, properties):
    # context目前不写死它，而是作为参数传入，因为如果有多个customer journey，这个需要变,
     #比如后面customer_id有C_10001,C_10002等等，届时可以写context1,context2等
    event=context.copy()
    event['event_id']=_identifier(context, "event/" + event_type)
    event["event_type"]=event_type
    event["event_time"]=event_time
    event["properties"]=properties
    return event



def _identifier(context, name):
    if "run_id" in context:
        return str(uuid5(UUID(context["session_id"]), name))
    return str(uuid4())


def create_journey(context, *, rng=None, base_time=None):

    rng = random if rng is None else rng
    base_time = datetime.now(timezone.utc) if base_time is None else base_time

    onboarding_started=create_event(
        context,
        "onboarding_started",
        base_time.isoformat(),
        {
            "campaign_id":"CMP_001",
            "entry_point":"home_banner"
        }
    )


    profile_submitted=create_event(
        context,
        "profile_submitted",
        (base_time+timedelta(minutes=3)).isoformat(),
        {
            "country":"SG",
            "occupation_group":"professional"
        }
    )
    kyc_submitted=create_event(
        context,
        "kyc_submitted",
        (base_time+timedelta(minutes=5)).isoformat(),
        {
            "kyc_method":"document_selfie",

        }
    )

    kyc_verification_completed=create_event(
        context,
        "kyc_verification_completed",
        (base_time+timedelta(minutes=10)).isoformat(),
        {"status":"passed",
         "review_type":"automated"}

    )

    account_opened=create_event(
        context,
        "account_opened",
        (base_time+timedelta(minutes=12)).isoformat(),
        {"account_id":_identifier(context, "account")}

    )
    card_activated=create_event(
        context,
        "card_activated",
        (base_time+timedelta(minutes=15)).isoformat(),
        {"card_type":"debit card",
        "card_id":_identifier(context, "card"),}
    )

    first_transaction_completed=create_event(
        context,
        "first_transaction_completed",
        (base_time+timedelta(minutes=20)).isoformat(),
        {"amount": 30.00,
         "currency": "USD",
         "transaction_type":"card_purchase",
         "transaction_id":_identifier(context, "transaction"),}
    )

    journey=[
        onboarding_started,
        profile_submitted,
        kyc_submitted,
        kyc_verification_completed,
        account_opened,
        card_activated,
        first_transaction_completed
    ]

    # Simulate different onboarding outcomes
    final_stage_index = rng.choices(
        population=range(len(journey)),
        weights=[5, 10, 10, 10, 10, 15, 40],
        k=1
    )[0]

    return journey[:final_stage_index + 1]
#


def generate_events(spec: SimulationSpec):
    """One distinct synthetic customer and one onboarding session per batch slot."""
    rng = random.Random(spec.seed)  # Never change the process-wide random state.
    for customer_number in range(1, spec.customer_count + 1):
        context = create_customer_context(customer_number, rng=rng, run_id=spec.run_id)
        context.update({"generator_version": GENERATOR_VERSION,
                        "simulation_seed": spec.seed,
                        "simulation_base_time": spec.base_time.isoformat()})
        started_at = spec.base_time + timedelta(
            seconds=(customer_number - 1) * CUSTOMER_START_INTERVAL_SECONDS
        )
        yield from create_journey(context, rng=rng, base_time=started_at)


if __name__ == "__main__":
    for event in generate_events(SimulationSpec.create(customer_count=10)):
        print(event)
