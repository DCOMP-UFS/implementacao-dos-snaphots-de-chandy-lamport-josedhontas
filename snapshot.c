/**
 * 
 * Compilação: mpicc -o snapshot snapshot.c
 * Execução:   mpiexec -n 3 ./snapshot
 */
 
#include <stdio.h>
#include <string.h>  
#include <mpi.h>     
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <semaphore.h>

#define BufferSize 20 // Size of the buffer

sem_t empty;
sem_t full;
pthread_mutex_t mutex;

sem_t empty1;
sem_t full1;
pthread_mutex_t mutex1;


sem_t snapempty;
sem_t snapfull;
pthread_mutex_t snapmutex;

pthread_mutex_t clockmutex;

int in1 = 0;
int out1 = 0;
int tamanho=0;

int in2 = 0;
int out2 = 0;

typedef struct 
{
    int pid;
    int destination;
    int p[3];
} Clock;



int pid;

Clock bufferin[BufferSize];
Clock bufferout[BufferSize];

Clock channel1;

Clock channel2;

Clock clock1;

Clock marker;

int snapativo=0;
int markcounter=0;

void snapshot();
void comparaClocks(Clock *clockv,int p0, int p1, int p2 );

void producer1(Clock c)
{   
        Clock item1=c;
        sem_wait(&empty);
        pthread_mutex_lock(&mutex);
        bufferin[in1] = item1;
        in1 = (in1+1)%BufferSize;
        pthread_mutex_unlock(&mutex);
        sem_post(&full);

}

Clock consumer1()
{   
        sem_wait(&full);
        pthread_mutex_lock(&mutex);
        Clock item2 = bufferin[out1];
        out1 = (out1+1)%BufferSize;
        pthread_mutex_unlock(&mutex);
        sem_post(&empty);
        return item2;
    
}

void producer2(Clock c)
{   
        Clock item3=c;
        sem_wait(&empty1);
        pthread_mutex_lock(&mutex1);
        bufferout[in2] = item3;
        tamanho++;
        in2 = (in2+1)%BufferSize;
        pthread_mutex_unlock(&mutex1);
        sem_post(&full1);

}
Clock consumer2()
{   
        sem_wait(&full1);
        pthread_mutex_lock(&mutex1);
        Clock item4 = bufferout[out2];
        out2 = (out2+1)%BufferSize;
        tamanho--;
        pthread_mutex_unlock(&mutex1);
        sem_post(&empty1);
        return item4;
    
}

void Event(int pid, Clock *clockt){
   pthread_mutex_lock(&clockmutex);
   clockt->p[pid]++;
   pthread_mutex_unlock(&clockmutex);
}

void *Emissor (){
   while(1){
   Clock c = consumer1();
   MPI_Send(c.p,3,MPI_INT,c.destination,0,MPI_COMM_WORLD);
    }
}

void *ReceptorSnap (){
   while(1){
   Clock clock;
   MPI_Status status;
   
   pthread_mutex_lock(&clockmutex);
   int antigo0 = clock1.p[0];
   int antigo1 = clock1.p[1];
   int antigo2 = clock1.p[2];
   pthread_mutex_unlock(&clockmutex);
   
   MPI_Recv(clock.p, 3, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
   
   if(clock.p[0]==-1){
       
       if(pid!=0){
          snapshot();
       }else{
       markcounter++;
       if(markcounter==2){
           snapativo=0;
       }
       }
   }else if(snapativo==1&&markcounter<2){
       if(status.MPI_SOURCE==1){
           channel1=clock;
           printf("channel P1->P0 : {%d, %d, %d}\n",channel1.p[0],channel1.p[1],channel1.p[2]);
           comparaClocks(&clock,antigo0,antigo1,antigo2);
           producer2(clock);
           printf("channel P2->P0 : vazio");
       }else if (status.MPI_SOURCE==2){
           channel2=clock;
           printf("channel P2->P0 : {%d, %d, %d}\n",channel2.p[0],channel2.p[1],channel2.p[2]);
           comparaClocks(&clock,antigo0,antigo1,antigo2);
           producer2(clock);
           printf("channel P1->P0 : vazio");
       }
   }
   else{
   comparaClocks(&clock,antigo0,antigo1,antigo2);
   producer2(clock);
   }
   }
}

void snapshot(){
pthread_mutex_lock(&clockmutex);
Clock c = clock1;

printf("SNAPSHOT: clock salvo de %d é (%d, %d, %d)\n", pid, c.p[0], c.p[1], c.p[2]);

if(pid==0){
    marker.destination=1;
    producer1(marker);
    
    marker.destination=2;
    producer1(marker);
        
}
if(pid==1){
    printf("channel P0->P1 : vazio\n");
    marker.destination=0;
    producer1(marker);
}
if(pid==2){
    printf("channel P0->P2 : vazio\n");
    marker.destination=0;
    producer1(marker); 
}

pthread_mutex_unlock(&clockmutex);
}



void comparaClocks(Clock *clockv,int p0, int p1, int p2 )
{
   if(clockv->p[0]<=p0){
      clockv->p[0]=p0;
   }
   if(clockv->p[1]<=p1){
      clockv->p[1]=p1;
   }
   if(clockv->p[2]<=p2){
      clockv->p[2]=p2;
   }
   
}

// Representa o processo de rank 0
void process0(){
   pid =0;
   clock1.p[0]=0;
   clock1.p[1]=0;
   clock1.p[2]=0;
   
   marker.p[0]=(-1);
   marker.p[1]=(-1);
   marker.p[2]=(-1);
   
   channel1.p[0]=(-1);
   channel2.p[0]=(-1);
   
    pthread_t pro,con;
    pthread_mutex_init(&mutex, NULL);
    sem_init(&empty,0,BufferSize);
    sem_init(&full,0,0);
    
    pthread_mutex_init(&mutex1, NULL);
    sem_init(&empty1,0,BufferSize);
    sem_init(&full1,0,0);
    
    pthread_mutex_init(&snapmutex, NULL);
    sem_init(&snapempty,0,BufferSize);
    sem_init(&snapfull,0,0);
    
    pthread_mutex_init(&clockmutex, NULL);
    
    pthread_create(&pro, NULL, (void *)Emissor, NULL);
    
    pthread_create(&con, NULL, (void *)ReceptorSnap, NULL);

   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   clock1.destination=1;
   producer1(clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
    
   clock1=consumer2();
   Event(pid, &clock1);
   
   printf("<SNAPSHOT INICIADO>\n");
   snapativo=1;
   snapshot();
   
   while(snapativo){
       
   }
   printf("<SNAPSHOT FINALIZAO>\n");
   
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   clock1.destination=2;
   producer1(clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   clock1=consumer2();
   Event(pid, &clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   
   Event(pid, &clock1);
   clock1.destination=1;
   producer1(clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 0, clock1.p[0], clock1.p[1], clock1.p[2]);
   
    
    pthread_join(pro, NULL);
    pthread_join(con, NULL);

    pthread_mutex_destroy(&mutex);
    sem_destroy(&empty);
    sem_destroy(&full);
    
    pthread_mutex_destroy(&mutex1);
    sem_destroy(&empty1);
    sem_destroy(&full1); 
    
    pthread_mutex_destroy(&snapmutex);
    sem_destroy(&snapempty);
    sem_destroy(&snapfull); 
    
    pthread_mutex_destroy(&clockmutex);

}
void process1(){
   pid =1;
   clock1.p[0]=0;
   clock1.p[1]=0;
   clock1.p[2]=0;
   
   marker.p[0]=(-1);
   marker.p[1]=(-1);
   marker.p[2]=(-1);
   
    pthread_t pro,con;
    pthread_mutex_init(&mutex, NULL);
    sem_init(&empty,0,BufferSize);
    sem_init(&full,0,0);
    
    pthread_mutex_init(&mutex1, NULL);
    sem_init(&empty1,0,BufferSize);
    sem_init(&full1,0,0);
    
    pthread_mutex_init(&snapmutex, NULL);
    sem_init(&snapempty,0,BufferSize);
    sem_init(&snapfull,0,0);
    
    pthread_mutex_init(&clockmutex, NULL);
        
    
    pthread_create(&pro, NULL, (void *)Emissor, NULL);
    
    pthread_create(&con, NULL, (void *)ReceptorSnap, NULL);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 1, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   clock1.destination=0;
   producer1(clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 1, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   clock1=consumer2();
   Event(pid, &clock1);

   printf("Process: %d, Clock: (%d, %d, %d)\n", 1, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   clock1=consumer2();
   Event(pid, &clock1);

   printf("Process: %d, Clock: (%d, %d, %d)\n", 1, clock1.p[0], clock1.p[1], clock1.p[2]);
   
    pthread_join(pro, NULL);
    pthread_join(con, NULL);

    pthread_mutex_destroy(&mutex);
    sem_destroy(&empty);
    sem_destroy(&full);
    
    pthread_mutex_destroy(&mutex1);
    sem_destroy(&empty1);
    sem_destroy(&full1); 
    
    pthread_mutex_destroy(&snapmutex);
    sem_destroy(&snapempty);
    sem_destroy(&snapfull); 
    
    pthread_mutex_destroy(&clockmutex);
}

// Representa o processo de rank 2
void process2(){
   pid = 2;
   clock1.p[0]=0;
   clock1.p[1]=0;
   clock1.p[2]=0;
   
   marker.p[0]=(-1);
   marker.p[1]=(-1);
   marker.p[2]=(-1);
   
   pthread_t pro,con;
    pthread_mutex_init(&mutex, NULL);
    sem_init(&empty,0,BufferSize);
    sem_init(&full,0,0);
    
    pthread_mutex_init(&mutex1, NULL);
    sem_init(&empty1,0,BufferSize);
    sem_init(&full1,0,0);
    
    pthread_mutex_init(&snapmutex, NULL);
    sem_init(&snapempty,0,BufferSize);
    sem_init(&snapfull,0,0);
    
    pthread_mutex_init(&clockmutex, NULL);
        
    
    pthread_create(&pro, NULL, (void *)Emissor, NULL);
    
    pthread_create(&con, NULL, (void *)ReceptorSnap, NULL);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 2, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 2, clock1.p[0], clock1.p[1], clock1.p[2]);
   
   Event(pid, &clock1);
   clock1.destination=0;
   producer1(clock1);
   
   printf("Process: %d, Clock: (%d, %d, %d)\n", 2, clock1.p[0], clock1.p[1], clock1.p[2]);

   clock1=consumer2();
   Event(pid, &clock1);

   printf("Process: %d, Clock: (%d, %d, %d)\n", 2, clock1.p[0], clock1.p[1], clock1.p[2]);


    pthread_join(pro, NULL);
    pthread_join(con, NULL);

    pthread_mutex_destroy(&mutex);
    sem_destroy(&empty);
    sem_destroy(&full); 
    
    pthread_mutex_destroy(&mutex1);
    sem_destroy(&empty1);
    sem_destroy(&full1); 
    
    pthread_mutex_destroy(&snapmutex);
    sem_destroy(&snapempty);
    sem_destroy(&snapfull); 
    
    pthread_mutex_destroy(&clockmutex);
}



int main(void) {
   int my_rank;    
   

   MPI_Init(NULL, NULL); 
   MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); 

   if (my_rank == 0) { 
      process0();
   } else if (my_rank == 1) {  
      process1();
   } else if (my_rank == 2) {  
      process2();
   }

   /* Finaliza MPI */
   MPI_Finalize(); 
   
   return 0;
}  /* main */