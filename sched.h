#pragma once
#include <vector>
#include <array>
#include <atomic>
#include <cassert>


//fwd declaration
namespace std {
	class mutex;
	class condition_variable;
}
namespace sched {

	struct Job {
		enum class JobState : char {
			created = 0,
			scheduled = 1,
			executing = 2,
			finished = 3
		};
		
		void init() {
			finishState = JobState::created;
			finishedAtomic = nullptr;
			fn = nullptr;
			for (auto& c : padding)
			{
				c = 0;
			}
		}

		std::atomic<int>* finishedAtomic;
		void (*fn)(Job*);
		std::atomic<JobState> finishState{ JobState::created };

		//pad the job struct to 64 bytes, to match a cacheline
		static constexpr std::size_t JOB_PAYLOAD_SIZE = sizeof(finishedAtomic)
			+ sizeof(finishState)
			+ sizeof(fn);

		static constexpr std::size_t PADDING_SIZE = 64;

		static constexpr std::size_t JOB_PADDING_SIZE = PADDING_SIZE - JOB_PAYLOAD_SIZE;

		std::array<char, JOB_PADDING_SIZE> padding;
		
		bool is_finished() const {
			return finishState == JobState::finished;
		}

		template<typename Data>
		const Data& get_data() const
		{
			return *reinterpret_cast<const Data*>((void*)padding.data());
		}

		template<typename F>
		void set_callback(F functor)
		{
			static_assert(sizeof(F) < JOB_PADDING_SIZE,"Functor is too big");
			
			fn = +[](Job* self) {
				
				const F& function = self->get_data<F>();
				function();

				void* pad = self->padding.data();
				F* padf = (F*)pad;
				padf->~F();
			};
			
			void* pad = padding.data();
			F* padf = new (pad) F{functor};
		}



		void run();
	};
	
	enum class ExecutionMode: char {
		Singlethread,
		Chunked,
		Recursive
	};
	struct Worker;
	
	struct Scheduler {

		~Scheduler();

		std::atomic<int> lastworker{0};

		std::atomic<int> enqueued_jobs;

		std::atomic<int> num_parked;

		std::mutex* park_mutex;
		std::condition_variable* parkvar;

		bool end{false};

		void park();

		void signal_workers();

		size_t get_worker_id();

		void enqueue_job(Job* jb);

		void wait_job(Job* jb);

		void run_job(Job* jb, bool enqueued);

		void run_worker();

		bool run_worker_once(size_t id);

		void launch_workers(int count);
		void allocate_threads(int workercount);

		void bulk_enqueue(Job** first, size_t count);

		void wait_atomic(std::atomic<int>* counter, int num);

		template<typename Fa, typename Fb>
		void split(Fa&& functionA, Fb&& functionB) {
			
			std::atomic<int> counter = 2;
			Job ja{};
			Job jb{};
			{
				//ZoneScopedNC("Allocing job",tracy::Color::Blue);
				ja.finishedAtomic = &counter;
				ja.set_callback(functionA);

				jb.finishedAtomic = &counter;
				jb.set_callback(functionB);
			}

			Job* jobs[2] = { &ja,&jb };

			bulk_enqueue(jobs, 2);

			wait_atomic(&counter,0);
		}
		template<typename Fa, typename Fb, typename Fc>
		void split3(Fa&& functionA, Fb&& functionB, Fc&& functionC) {

			std::atomic<int> counter = 3;
			Job ja{};
			Job jb{};
			Job jc{};
			{
				//ZoneScopedNC("Allocing job",tracy::Color::Blue);
				ja.finishedAtomic = &counter;
				ja.set_callback(functionA);

				jb.finishedAtomic = &counter;
				jb.set_callback(functionB);

				jc.finishedAtomic = &counter;
				jc.set_callback(functionC);
			}

			Job* jobs[3] = { &ja,&jb,&jc };

			bulk_enqueue(jobs, 3);

			wait_atomic(&counter, 0);
		}


		std::vector<Worker*> workers;
	};

	namespace algo {
		template<typename F>
		void parallel_for(Scheduler* sch, int start, int end, int batch, F&& function, ExecutionMode mode = ExecutionMode::Recursive);

		template<typename F>
		void parallel_for_recursive(Scheduler* sch, int start, int end, int batch, F&& function) {

			int range = end - start;
			assert(range >= 0);

			if (range > batch) {

				int midpoint = (end - start) / 2 + start;

				sch->split([&]() {
						parallel_for_recursive(sch, start, midpoint, batch, function);
					},
						[&]() {
						parallel_for_recursive(sch, midpoint, end, batch, function);
					}
				);
			}
			else {
				parallel_for(sch, start, end, batch, function, ExecutionMode::Singlethread);
			}
		}
		template<typename F>
		void parallel_for_atomic(Scheduler* sch, int start, int end, int batch, F&& function) {
			int nbatches = ((end - start) / batch) + 1;
		
			sched::Job localjobs[16];
			sched::Job* jobpointers[16];
			std::atomic<int> batchcounter{0};
			
			int jobs = 16;
			if (nbatches < jobs) {
				jobs = nbatches;
			}

			std::atomic<int> jobend = jobs;

			for (int i = 0; i < jobs; i++)
			{
				localjobs[i].init();
				jobpointers[i] = localjobs + i;
				localjobs[i].finishedAtomic = &jobend;

				auto callback = [=,&batchcounter, &function]() {
					while (true)
					{
						int idx = batchcounter.fetch_add(1);
						if (idx > nbatches) { return; };
						int _pack = batch;
						int _begin = _pack * idx;
						int _end = _pack * (idx + 1);
						if (_end > end)
						{
							_end = end;
						}
						if (_begin != _end)
						{
							parallel_for(sch, _begin, _end, 1, function, ExecutionMode::Singlethread);
						}
					}
				};
				localjobs[i].set_callback(callback);
			}

			sch->bulk_enqueue(jobpointers, jobs);
			sch->wait_atomic(&jobend,0);
		}

		template<typename F>
		void parallel_for(Scheduler * sch, int start, int end, int batch, F && function, ExecutionMode mode) {

			if (mode == ExecutionMode::Recursive) {

				parallel_for_recursive(sch, start, end, batch, function);
			}
			else if (mode == ExecutionMode::Chunked)
			{
				parallel_for_atomic(sch, start, end, batch, function);
			}
			else {
				//ZoneScopedNC("RUN JOB", tracy::Color::Red);
				for (int i = start; i < end; i++)
				{
					function(i);
				}
			}
		}
	}
}