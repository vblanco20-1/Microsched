#include <sched.h>
#include <condition_variable>

#define PROFILE_LEVEL 0
#ifdef TRACY_ENABLE
#include <Tracy.hpp>
#define PROFILE_LEVEL 2
#endif

#include <mutex>
#include <thread>

#include <iostream>
#include "intrin.h"
using namespace sched;
namespace sched {
	struct RNG {
		inline uint64_t rotl(const uint64_t x, int k) {
			return (x << k) | (x >> (64 - k));
		}

		uint64_t s[2];

		uint64_t next(void) {
			const uint64_t s0 = s[0];
			uint64_t s1 = s[1];
			const uint64_t result = s0 + s1;

			s1 ^= s0;
			s[0] = rotl(s0, 24) ^ s1 ^ (s1 << 16); // a, b
			s[1] = rotl(s1, 37); // c

			return result;
		}
	};


	//benchmarked to be a lot faster than std::mutex for this use case
	class alignas (sched::CACHELINE_SIZE) SpinLock {
		std::atomic<bool> lock_ = { false };

	public:
		void lock() {
			for (;;) {
				if (!lock_.exchange(true, std::memory_order_acquire)) {
					break;
				}
				while (lock_.load(std::memory_order_relaxed)) {
					std::this_thread::yield();
				}
			}
		}
		void unlock() {
			lock_.store(false, std::memory_order_release);
		}
	};
	template<typename T, int N>
	struct RingQueue {
		SpinLock mtx;
		SpinLock popmtx;

		int head{ 0 };
		int tail{ 0 };
		int items{ 0 };

		static constexpr int ring_size = N;

		T queue[N];
		bool pop(T& item)
		{
			mtx.lock();

			bool ret = false;

			if (head != tail)
			{
				int idx = tail;
				item = queue[idx];
				queue[idx] = {};

				tail = (tail + 1) % N;
				ret = true;
			}

			mtx.unlock();

			return ret;
		};
		void bulk_push(T* first, int count)
		{
			mtx.lock();
			for (int i = 0; i < count; i++)
			{
				queue[head] = first[i];
				head = (head + 1) % N;
			}
			mtx.unlock();
		}
		void push(T& item) {

			mtx.lock();
			queue[head] = item;
			head = (head + 1) % N;
			mtx.unlock();
		};

		bool pop_steal(T& item) {
			return pop(item);
		};
	};

	struct Worker {
		RNG random;
		struct Scheduler* owner;

		std::atomic<int> initialized{ 0 };
		std::atomic<int> task_count{ 0 };
		
		RingQueue<Job*, 512> jobs{};

		Job* grab_job();
		void add_job(Job* jb);
		void bulk_add(Job** first, size_t count);
	};

}


sched::Scheduler::~Scheduler()
{
	end = true;
	signal_workers();
}

void sched::Scheduler::park()
{
	{
		int id = num_parked.fetch_add(1,std::memory_order_release);
		#if PROFILE_LEVEL > 0
		ZoneScopedN("PARKED");
		#endif

		// by locking the mutex here the threads will all wait here except the parked one

		std::unique_lock<std::mutex> lk(*park_mutex);

		//if we find there are jobs around after the mutex, dont park
		if (enqueued_jobs > 0)
		{
			num_parked--;
			return;
		}
		else {
			//park the thread. Only one will
			parkvar->wait(lk);

			num_parked--;
		}
	}
}

void sched::Scheduler::signal_workers()
{
	//check atomic first as a quick check
	if(num_parked.load(std::memory_order::memory_order_relaxed) != 0)
	{
#if PROFILE_LEVEL > 0
		ZoneScopedN("Signal Workers");
#endif
		
		parkvar->notify_all();
	}
}

size_t sched::Scheduler::get_worker_id()
{
	//each thread has a local ID, we grab a worker data from the scheduler as requested
	thread_local int id = -1;

	if (id == -1)
	{
		id = lastworker.fetch_add(1);
		workers[id]->initialized = 1;
		workers[id]->owner = this;
	}

	return id;
}

void sched::Scheduler::enqueue_job(Job* jb)
{
#if PROFILE_LEVEL > 1
	ZoneScoped;
#endif
	
	auto id = get_worker_id();
	workers[id]->add_job(jb);
}

void sched::Scheduler::wait_job(Job* jb)
{
#if PROFILE_LEVEL > 0
	ZoneScoped;
#endif
	//run the thread as a worker until the job is finished. This does not park the thread

	auto id = get_worker_id();

	while (!jb->is_finished())
	{
		bool empty = run_worker_once(id);

		if (empty == true){
#if PROFILE_LEVEL > 1
			ZoneScopedN("Local Yield");
#endif
			
			std::this_thread::yield();
		}
	}
}

void sched::Scheduler::run_job(Job* jb, bool enqued)
{
	jb->run();
}

void sched::Scheduler::run_worker()
{
#if PROFILE_LEVEL > 0
	ZoneScoped;
#endif

	auto id = get_worker_id();
	int attempts = 0;

	//endless worker loop
	while (!end)
	{		
		{
#if PROFILE_LEVEL > 0
			ZoneScopedN("Worker Loop");
#endif
			
			//attempt to run the worker in a loop for a few times
			//busy loop
			while (attempts < 100)
			{
				bool empty = run_worker_once(id);

				if (empty == true)
				{
					attempts++;

					std::this_thread::yield();
				}
				else {
					attempts = 0;
				}
			}
		}

		//if we havent found any job to execute for a lot of iterations, get it parked
		{
#if PROFILE_LEVEL > 0
			ZoneScopedN("Worker Wait");
#endif
			//parking loop
			while (enqueued_jobs.load(std::memory_order::memory_order_relaxed) == 0)
			{
				park();
				attempts = 0;
			}
		}
	}
}

bool sched::Scheduler::run_worker_once(size_t id)
{
	//ZoneScoped;
	bool empty = true;
	//grab job from local queue
	Job* self = workers[id]->grab_job();
	if (self)
	{
		//execute local job
		empty = false;
		run_job(self,true);
	}
	if (empty)
	{
		//local queue is empty, select random worker to steal from
		int rng = workers[id]->random.next() % workers.size();
		
		Worker* worker = workers[rng];
		{
			if (worker && worker->initialized == 1)
			{
				Job* jb = worker->grab_job();
				if (jb)
				{
#if PROFILE_LEVEL > 1
					ZoneScopedN("WorkSteal");
#endif
					
					empty = false;
					run_job(jb,true);
				}
			}
		}
	}
	return empty;
}

void sched::Scheduler::launch_workers(int count)
{
	for (int i = 0; i < count; i++)
	{
		std::thread worker(&Scheduler::run_worker, this);
		worker.detach();
	}
}

void sched::Scheduler::allocate_threads(int workercount)
{
	workers.resize(workercount);

	for (int i = 0; i < workercount;i++)
	{
		workers[i] = (new Worker{});
		workers[i]->random.s[1] = rand();
		workers[i]->random.s[2] = rand();
		workers[i]->initialized = 0;
		workers[i]->task_count = 0;
	}

	park_mutex = new std::mutex;
	parkvar = new std::condition_variable;
}

void sched::Scheduler::bulk_enqueue(Job** first, size_t count)
{
#if PROFILE_LEVEL > 1
	ZoneScoped;
#endif
	//if the amount of jobs is small, just add them to the local queue and let workstealing do its thing
	int self = get_worker_id();
	if (count < 16)
	{
		workers[self]->bulk_add(first, count);
		return;
	}

	//grab some workers to enqueue jobs into
	Worker* validworkers[16];
	int num = 0;

	for (int i = 0; i < workers.size(); i++)
	{
		if (workers[i]->initialized && i != self)
		{
			validworkers[num] = workers[i];
			num++;
		}
	}
		
	//enqueue a few of the jobs in the current thread
	int idx = count /2;
	if (idx > 16)
	{
		idx = 16;
	}
	workers[self]->bulk_add(first, idx);

	//enqueue the rest of the jobs spread across random workers 16 at a time
	int rng = rand() % num;

	while (idx < count)
	{
		int left = count - idx;
		int bulk = 16;
		if (left < 16) {
			bulk = left;
		}

		validworkers[rng]->bulk_add(first + idx, bulk);
		idx += bulk;

		rng = (rng + 1) % num;
	}
}

void sched::Scheduler::wait_atomic(std::atomic<int>* counter, int num)
{
#if PROFILE_LEVEL > 0
	ZoneScopedNC("Wait Atomic", tracy::Color::White);
#endif
	
	//keep iterating until the atomic is met. This does not park the worker

	while (counter->load() != num)
	{
		int id = get_worker_id();
		run_worker_once(id);
		signal_workers();
		std::this_thread::yield();
	}
}

sched::Job* sched::Worker::grab_job()
{
	Job* jb = nullptr;

	//check the task count atomic first
	int count = task_count.load();

	if (count == 0) return nullptr;

	//ringbuffer not empty, pop a job
	if (jobs.pop(jb))
	{
		owner->enqueued_jobs--;
		task_count--;
	}

	return jb;
}
void sched::Worker::add_job(Job* jb)
{
#if PROFILE_LEVEL > 1
	ZoneScoped;
#endif

	//execute stuff inline if we find we might overflow
	if (task_count >= jobs.ring_size - 30)
	{
		owner->run_job(jb,false);
	}
	else {
		int j = owner->enqueued_jobs.load();
		jb->finishState = Job::JobState::scheduled;
		task_count++;
		owner->enqueued_jobs++;
		jobs.push(jb);
		
		if (j == 0)
		{
			owner->signal_workers();
		}
	}	
}

void sched::Worker::bulk_add(Job** first, size_t count)
{
#if PROFILE_LEVEL > 1
	ZoneScoped;
#endif	
	//if the ringbuffer is near full, execute the jobs inline without enqueuing them
	int left = jobs.ring_size - (task_count + count);
	if (left < 30)
	{
		//run inline
		for (int i = 0; i < count; i++)
		{
			owner->run_job(first[i],false);
		}
	}
	else {

		//enqueue the jobs
		for (int i = 0; i < count; i++)
		{
			first[i]->finishState = Job::JobState::scheduled;
		}

		task_count += count;
		int j = owner->enqueued_jobs.fetch_add(count);
		jobs.bulk_push(first, count);
		
		
		if (j == 0)
		{
			owner->signal_workers();
		}
	}
}

void sched::Job::run()
{
	assert(finishState == JobState::created || finishState == JobState::scheduled);
	assert(fn != nullptr);

	finishState = JobState::executing;

#if PROFILE_LEVEL > 1
	ZoneScopedNC("RUN JOB", tracy::Color::Red);
#endif
	//execute job callback
	fn(this);

	
	std::atomic<int>* finalatomic = finishedAtomic;	

	//clear the job
	finishState = JobState::finished;
	fn = nullptr;
	finishedAtomic = nullptr;
	memset(padding.data(), 0xFF, sizeof(padding));

	//call the finish atomic
	if (finalatomic)
	{
		(*finalatomic).fetch_sub(1);
	}
}
