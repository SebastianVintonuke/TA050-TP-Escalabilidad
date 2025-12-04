import sys
import time

from supervisor.src.leader_election import BullyElection, SupervisorState

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def add_pass(self, test_name):
        self.passed += 1
        print(f"✓ {test_name}")

    def add_fail(self, test_name, error):
        self.failed += 1
        self.errors.append((test_name, error))
        print(f"✗ {test_name}: {error}")

    def summary(self):
        total = self.passed + self.failed
        print("\n" + "="*60)
        print(f"Resultados: {self.passed}/{total} tests pasaron")
        if self.failed > 0:
            print(f"\nTests fallidos ({self.failed}):")
            for name, error in self.errors:
                print(f"  - {name}: {error}")
        print("="*60)
        return self.failed == 0


def assert_equal(actual, expected, msg=""):
    if actual != expected:
        raise AssertionError(f"{msg} - Esperado: {expected}, Actual: {actual}")


def assert_true(condition, msg=""):
    if not condition:
        raise AssertionError(f"{msg} - Condición falsa")


def assert_not_none(value, msg=""):
    if value is None:
        raise AssertionError(f"{msg} - Valor es None")


def wait_for_condition(condition_func, timeout=3.0, check_interval=0.1):
    start = time.time()
    while time.time() - start < timeout:
        if condition_func():
            return True
        time.sleep(check_interval)
    return False


def create_supervisor(supervisor_id, all_peers, election_timeout=3.0, alive_timeout=5.0):
    bully = BullyElection(
        supervisor_id=supervisor_id,
        supervisor_peers=all_peers,
        election_port=6000 + supervisor_id,
        election_timeout=election_timeout,
        alive_interval=1.0,
        alive_timeout=alive_timeout,
    )
    return bully


def stop_supervisor(bully):
    try:
        bully.stop()
    except Exception:
        pass

def test_highest_id_becomes_leader():    
    peers = {
        1: ("localhost", 6001),
        2: ("localhost", 6002),
        3: ("localhost", 6003),
    }
    
    s1 = create_supervisor(1, peers)
    s2 = create_supervisor(2, peers)
    s3 = create_supervisor(3, peers)
    
    supervisors = [s1, s2, s3]
    
    try:
        s1.start()
        s2.start()
        s3.start()
        
        # Esperar eleccion
        time.sleep(5.0)
        
        # S3 debe ser líder
        assert_equal(s3.state, SupervisorState.LEADER, "S3 debe ser LEADER")
        assert_equal(s3.current_leader, 3, "S3 debe reportar current_leader=3")
        
        # S1 y S2 deben ser followers del líder 3
        assert_equal(s1.state, SupervisorState.FOLLOWER, "S1 debe ser FOLLOWER")
        assert_equal(s1.current_leader, 3, "S1 debe conocer líder=3")
        
        assert_equal(s2.state, SupervisorState.FOLLOWER, "S2 debe ser FOLLOWER")
        assert_equal(s2.current_leader, 3, "S2 debe conocer líder=3")
        
    finally:
        for s in supervisors:
            stop_supervisor(s)


def test_leader_crash_triggers_reelection():    
    peers = {
        1: ("localhost", 6001),
        2: ("localhost", 6002),
        3: ("localhost", 6003),
    }
    
    s1 = create_supervisor(1, peers, alive_timeout=3.0)
    s2 = create_supervisor(2, peers, alive_timeout=3.0)
    s3 = create_supervisor(3, peers, alive_timeout=3.0)
    
    try:
        s1.start()
        s2.start()
        s3.start()
        
        time.sleep(5.0)
        assert_equal(s3.state, SupervisorState.LEADER, "S3 debe ser líder inicial")
        
        print("crash de S3 (líder)...")
        stop_supervisor(s3)
        
        time.sleep(7.0)
        
        assert_equal(s2.state, SupervisorState.LEADER, "S2 debe ser nuevo LEADER")
        assert_equal(s2.current_leader, 2, "S2 debe tener current_leader=2")
        
        assert_equal(s1.state, SupervisorState.FOLLOWER, "S1 debe ser FOLLOWER")
        assert_equal(s1.current_leader, 2, "S1 debe reconocer líder=2")
        
    finally:
        stop_supervisor(s1)
        stop_supervisor(s2)


def test_discover_existing_leader():    
    peers = {
        1: ("localhost", 6001),
        2: ("localhost", 6002),
    }
    
    s2 = create_supervisor(2, peers)
    
    try:
        s2.start()
        time.sleep(5.0)
        
        assert_equal(s2.state, SupervisorState.LEADER, "S2 debe ser LEADER")
        
        s1 = create_supervisor(1, peers)
        s1.start()
        time.sleep(3.0)
        
        assert_equal(s1.state, SupervisorState.FOLLOWER, "S1 debe ser FOLLOWER")
        assert_equal(s1.current_leader, 2, "S1 debe reconocer líder=2")
        
        assert_equal(s2.state, SupervisorState.LEADER, "S2 debe seguir siendo LEADER")
        
        stop_supervisor(s1)
        
    finally:
        stop_supervisor(s2)

def test_higher_id_challenges_lower_id():    
    peers = {
        1: ("localhost", 6001),
        3: ("localhost", 6003),
    }
    
    s1 = create_supervisor(1, peers)
    
    try:
        s1.start()
        time.sleep(4.0)
        assert_equal(s1.state, SupervisorState.LEADER, "S1 debe ser líder inicial")
        
        s3 = create_supervisor(3, peers)
        s3.start()
        time.sleep(4.0)
        
        # S3 debe convertirse en líder por mayor ID
        assert_equal(s3.state, SupervisorState.LEADER, "S3 debe ser nuevo LEADER")
        assert_equal(s3.current_leader, 3, "S3 debe reportar current_leader=3")
        
        # S1 debe ceder liderazgo y convertirse en follower
        assert_equal(s1.state, SupervisorState.FOLLOWER, "S1 debe ser FOLLOWER")
        assert_equal(s1.current_leader, 3, "S1 debe reconocer líder=3")
        
        stop_supervisor(s3)
        
    finally:
        stop_supervisor(s1)

def run_all_tests():
    results = TestResult()
    
    tests = [
        ("Test 1: ID mayor se autoproclamar líder", test_highest_id_becomes_leader),
        ("Test 2: Re-elección al caer líder", test_leader_crash_triggers_reelection),
        ("Test 3: Discovery de líder existente", test_discover_existing_leader),
        ("Test 4: ID mayor desafía y reemplaza líder", test_higher_id_challenges_lower_id),
    ]
    
    print("TESTS DEL ALGORITMO BULLY")
    
    for test_name, test_func in tests:
        try:
            test_func()
            results.add_pass(test_name)
        except AssertionError as e:
            results.add_fail(test_name, str(e))
        except Exception as e:
            results.add_fail(test_name, f"Error inesperado: {e}")
        
        time.sleep(1.0)
    
    success = results.summary()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
