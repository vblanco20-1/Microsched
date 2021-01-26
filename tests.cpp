// microsched.cpp : Defines the entry point for the application.
//

#include "sched.h"
#include <chrono>
#include <iostream>
#include <sstream>
using namespace std;

struct Benchy {
	std::chrono::high_resolution_clock::time_point start;

	void Start()
	{
		start = std::chrono::high_resolution_clock::now();
	};

	//in miliseconds
	double Time() {
		auto end = std::chrono::high_resolution_clock::now();

		auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

		//nanoseconds to miliseconds
		double time = duration.count() / 1000000.f;

		start = std::chrono::high_resolution_clock::now();
		return time;
	}
};

struct TestResult {

	bool success{false};
	std::string message{};
	float timing;

	void Print(std::string test_name) {
		std::string res = success ? "SUCCESS: " : "FAIL: ";

		cout << test_name << " - " << res << message << " Time: " << timing << "ms " << std::endl;
	}
};


TestResult test_simple(sched::Scheduler* sch) {

	Benchy bench;
	bench.Start();
	std::atomic<bool> test = false;

	sched::Job job{};
	job.set_callback([&]() {
		test = true;
	});


	sch->enqueue_job(&job);
	sch->wait_job(&job);

	double time = bench.Time();

	TestResult res{};
	res.timing = time;
	if (test == false)
	{
		res.message = "Job not exedcuted";
		res.success = false;
	}
	else
	{
		res.success = true;
	}
	return res;
}

TestResult test_multiple_linear(sched::Scheduler* sch, int count) {

	Benchy bench;
	bench.Start();
	std::atomic<int> test = 0;

	sched::Job* jobs = new sched::Job [count];

	std::atomic<int> waiter = count;

	for (int i = 0; i < count; i++)
	{
		jobs[i].init();
		jobs[i].set_callback([&]() {
			test++;
		});

		jobs[i].finishedAtomic = &waiter;
		sch->enqueue_job(&jobs[i]);
	}

	
	sch->wait_atomic(&waiter,0);

	double time = bench.Time();

	TestResult res{};
	res.timing = time;
	if (test != count)
	{
		res.message = "Job not executed correctly";
		res.success = false;
	}
	else
	{
		res.success = true;
	}
	
	delete[] jobs;
	return res;
}

TestResult test_multiple_batched(sched::Scheduler* sch, int count) {

	Benchy bench;
	bench.Start();
	std::atomic<int> test = 0;

	sched::Job* jobs = new sched::Job[count];

	std::atomic<int> waiter = count;
	std::vector<sched::Job*> enqueued;
	enqueued.resize(count);

	for (int i = 0; i < count; i++)
	{
		jobs[i].set_callback([&]() {
			test++;
			});

		jobs[i].finishedAtomic = &waiter;
		enqueued[i] = &jobs[i];
		//sch->enqueue_job(&jobs[i]);
	}

	sch->bulk_enqueue(enqueued.data(), enqueued.size());

	sch->wait_atomic(&waiter, 0);

	double time = bench.Time();

	TestResult res{};
	res.timing = time;
	if (test != count)
	{
		res.message = "Job not executed correctly";
		res.success = false;
	}
	else
	{
		res.success = true;
	}

	delete[] jobs;
	return res;
}


TestResult test_split_multi_batched(sched::Scheduler* sch, int count) {
	Benchy bench;
	bench.Start();

	TestResult a;
	TestResult b;
	sch->split(
		[&]() {
			a = test_multiple_batched(sch, count / 2);
		},
		[&]() {
			b = test_multiple_batched(sch, count / 2);
		}
	);

	TestResult res{};
	res.timing = bench.Time();
	if (a.success && b.success)
	{
		res.success = true;
	}
	else
	{
		res.message = "Job not executed correctly";
		res.success = false;
	}
	return res;
}
TestResult test_split_multi_linear(sched::Scheduler* sch, int count) {
	Benchy bench;
	bench.Start();

	TestResult a;
	TestResult b;
	sch->split(
		[&]() {
			a = test_multiple_linear(sch, count/2);
		},
		[&]() {
			b = test_multiple_linear(sch, count / 2);
		}
		);

	TestResult res{};
	res.timing = bench.Time();
	if (a.success && b.success)
	{
		res.success = true;
	}
	else
	{
		res.message = "Job not executed correctly";
		res.success = false;
	}
	return res;
}

//#pragma optimize( "", off )
double calcpi(int n) {
	double pi = 4.0, decimal = 1.0;
	while (n > 2)
	{
		decimal -= (1.0 / (2.0 * n + 1));
		--n;
		decimal += (1.0 / (2.0 * n + 1));
		--n;
	}
	if (n > 0)
		decimal -= (1.0 / (2.0 * n + 1));
	return(pi * decimal);
}
//#pragma optimize( "", on )

TestResult test_parallel_for(sched::Scheduler* sch, int count,int batch, sched::ExecutionMode mode) {
	Benchy bench;
	bench.Start();

	std::vector<double> pis;
	pis.resize(count);
	atomic<int> pin = 10000;
	sched::algo::parallel_for(sch, 0, count, batch, [&](int i) {
		pis[i] = calcpi(pin.load());

	}, mode);

	TestResult res{};
	res.timing = bench.Time();

	double first = pis[0];

	res.success = true;

	for (int i = 0; i < count; i++)
	{
		if (pis[i] != first)
		{
			res.message = "parallel for has different values";
			res.success = false;
			return res;
		}
	}
	return res;
}

//run all tests
TestResult mega_test(sched::Scheduler* sch, int count, int batch) {

	Benchy bench;
	bench.Start();

	TestResult parfor_ch;
	TestResult parfor_rec;

	TestResult linear;
	TestResult split;
	TestResult batched;
	sch->split(
		[&]() {
			sch->split(
				[&]() {
					parfor_rec = test_parallel_for(sch, count, batch, sched::ExecutionMode::Recursive);
				},
				[&]() {
					parfor_ch = test_parallel_for(sch, count, batch, sched::ExecutionMode::Chunked);
				}
			);
		},
		[&]() {
			sch->split3(
				[&]() {
					linear = test_multiple_linear(sch, count*10);
				}, [&]() {
					batched = test_multiple_linear(sch, count * 10);
				},
				[&]() {
					TestResult a;
					TestResult b;

					sch->split(
						[&]() {
							a = test_split_multi_linear(sch, count * 10);
						},
						[&]() {
							b = test_split_multi_batched(sch, count * 10);
						}
					);

					split.success = a.success && b.success;
			});
		}
	);

	TestResult res{};
	res.timing = bench.Time();
	if (parfor_ch.success && parfor_rec.success && 
		linear.success&& batched.success&& split.success		
		)
	{
		res.success = true;
	}
	else
	{
		res.message = "Job not executed correctly";
		res.success = false;
	}
	return res;

}

int main()
{
	sched::Scheduler sch{};
	sch.allocate_threads(16);
	sch.launch_workers(15);
	
	auto result1 = test_simple(&sch);
	result1.Print("test_simple");
	
	int jobcounts[8] = { 5,10,20,64,100,1000,10000,1'000'000};

	int num_retries = 3;

	for (int round = 0; round < 20; round++)
	{

		for (int i = 0; i < 8; i++)
		{
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_multiple_linear(&sch, jobcounts[i]);
				std::stringstream ss;
				ss << "test_multi_linear " << jobcounts[i];
				result1.Print(ss.str());
			}
		}

		for (int i = 0; i < 8; i++)
		{
			float time = 0;
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_multiple_batched(&sch, jobcounts[i]);
				std::stringstream ss;
				ss << "test_multi_batched " << jobcounts[i];
				result1.Print(ss.str());
			}
		}

		for (int i = 0; i < 8; i++)
		{
			float time = 0;
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_split_multi_batched(&sch, jobcounts[i]);
				std::stringstream ss;
				ss << "test_split_multi_batched " << jobcounts[i];
				result1.Print(ss.str());
			}
		}

		for (int i = 0; i < 8; i++)
		{
			float time = 0;
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_split_multi_linear(&sch, jobcounts[i]);
				std::stringstream ss;
				ss << "test_split_multi_linear " << jobcounts[i];
				result1.Print(ss.str());
			}
		}

		for (int i = 0; i < 7; i++)
		{
			int nbatches = (jobcounts[i] / 256) + 1;
			float time = 0;
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_parallel_for(&sch, jobcounts[i], nbatches, sched::ExecutionMode::Singlethread);
				std::stringstream ss;
				ss << "test_parallel_for_singlethread " << jobcounts[i];
				result1.Print(ss.str());
			}
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_parallel_for(&sch, jobcounts[i], nbatches, sched::ExecutionMode::Recursive);
				std::stringstream ss;
				ss << "test_parallel_for_recursive " << jobcounts[i];
				result1.Print(ss.str());
			}
			for (int n = 0; n < num_retries; n++)
			{
				auto result1 = test_parallel_for(&sch, jobcounts[i], nbatches, sched::ExecutionMode::Chunked);
				std::stringstream ss;
				ss << "test_parallel_for_chunked " << jobcounts[i];
				result1.Print(ss.str());
			}
		}
		for (int i = 0; i < 7; i++)
		{
			for (int n = 0; n < num_retries; n++)
			{
				int nbatches = (jobcounts[i] / 256) + 1;
				auto result1 = mega_test(&sch, jobcounts[i], nbatches);
				std::stringstream ss;
				ss << "megatest " << jobcounts[i];
				result1.Print(ss.str());
			}
		}
	}
	int n;
	cin >> n;
	return 0;
}
