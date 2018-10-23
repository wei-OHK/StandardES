//--- by WEI@OHK 2018-10-13 ---//
package SparkJob;

import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.Date;
import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class StandardES {

	public final static int VERSION_MAJOR = 2;
	public final static int VERSION_MINOR = 1;

	public static void main(String[] args) throws Exception {
		// Save job settings
		ESJobSettings settings = new ESJobSettings(args);

		//------------Log--------------------------------------------------------------//
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		FSDataOutputStream os_argsLog = FileSystem.get(new Configuration()).create(new Path(settings.getOutputPath() + "/jobSettings.log"), true);
		os_argsLog.write(("StandardESv" + VERSION_MAJOR + "." + VERSION_MINOR + " Application\n").getBytes("UTF-8"));
		for(String value : args) {
			os_argsLog.write((value+" ").getBytes("UTF-8"));
		}
		os_argsLog.write(("\n").getBytes("UTF-8"));
		os_argsLog.close();	
		//-----------------------------------------------------------------------------//

		// ES initialization
		Evaluator evaluator = new Evaluator(settings.getNativeLib(), settings.getFuncIndex());

		// Spark initialization
		JavaSparkContext sc = new JavaSparkContext(new SparkConf());

		// Create blank populations
		Population currentGen = new Population(settings.getLambda(), settings.getDimensions(), settings.getUcv(), settings.getLcv(), settings.getUsv(), settings.getLsv(), settings.getInitialsv());
		Population nextGen = new Population(settings.getLambda(), settings.getDimensions(), settings.getUcv(), settings.getLcv(), settings.getUsv(), settings.getLsv(), settings.getInitialsv());

		// Start!!!
		// Main Loop
		for(int i = 0; i < settings.getTrialsNum(); i++) {
			// Initialize the current generation at the beginning of each trial
			ESOperator.Initialize(currentGen);

			//------------Log--------------------------------------------------------------//
			FSDataOutputStream os_fitness = FileSystem.get(new Configuration()).create(new Path(settings.getOutputPath() + "/Trial_" + i + "/fitness.log"), true);
			FSDataOutputStream os_overview = FileSystem.get(new Configuration()).create(new Path(settings.getOutputPath() + "/Trial_" + i + "/overview.log"), true);
				
			double bestFitness = 0.0;
			//-----------------------------------------------------------------------------//

			for(int j = 0; j < settings.getMaxGeneration(); j++) {
				// Evaluation
				ESOperator.Evaluate(currentGen, sc, evaluator);
				// Parent Selection				
				List<Population.Individual> parents = ESOperator.SelectParent(currentGen, settings.getMu());
				// Recombination				
				ESOperator.Recombine(nextGen, parents);
				// Mutation				
				ESOperator.Mutate(nextGen);

				//------------Log--------------------------------------------------------------//
				double average_fitness = 0.0;
				for(Population.Individual k : currentGen.individuals) {
					average_fitness += k.fitness;
				}
				average_fitness /= currentGen.individuals.size();
				os_fitness.write((j+"\t"+currentGen.individuals.get(0).fitness+"\t"+average_fitness+"\n").getBytes("UTF-8"));
				os_fitness.hflush();
				
				os_overview.write(("******* Generation_"+j+" finished at "+df.format(new Date())+" *******\n").getBytes("UTF-8"));
				int indi_log_num = Math.min(settings.getLambda(), 5);
				os_overview.write(("Top "+indi_log_num+": ").getBytes("UTF-8"));
				for(int k = 0; k < indi_log_num; k++) 
					os_overview.write((currentGen.individuals.get(k).fitness+"  ").getBytes("UTF-8"));
				os_overview.write(("\n").getBytes("UTF-8"));
				os_overview.write(("Average: "+average_fitness+"\n").getBytes("UTF-8"));
				os_overview.hflush();

				FSDataOutputStream os_generation = FileSystem.get(new Configuration()).create(new Path(settings.getOutputPath() + "/Trial_" + i + "/generation_"+j+".log"), true);
				for(int k = 0; k < currentGen.individuals.size(); k++) {
					os_generation.write(("Individual_"+k+":\n").getBytes("UTF-8"));
					os_generation.write(("Fitness: "+currentGen.individuals.get(k).fitness+"\n").getBytes("UTF-8"));
					os_generation.write(("Chromosome: ").getBytes("UTF-8"));
					for(int l = 0; l < currentGen.individuals.get(k).chromosome.length; l++) {
						if(l > 0)
							os_generation.write((",").getBytes("UTF-8"));
						os_generation.write((Double.toString(currentGen.individuals.get(k).chromosome[l])).getBytes("UTF-8"));
					}
					os_generation.write(("\n").getBytes("UTF-8"));
					os_generation.write(("Strategy: ").getBytes("UTF-8"));
					for(int l = 0; l < currentGen.individuals.get(k).strategy.length; l++) {
						if(l > 0)
							os_generation.write((",").getBytes("UTF-8"));
						os_generation.write((Double.toString(currentGen.individuals.get(k).strategy[l])).getBytes("UTF-8"));
					}
					os_generation.write(("\n\n").getBytes("UTF-8"));
				}
				os_generation.close();
				
				if(currentGen.individuals.get(0).fitness > bestFitness) {
					bestFitness = currentGen.individuals.get(0).fitness;

					FSDataOutputStream os_best = FileSystem.get(new Configuration()).create(new Path(settings.getOutputPath() + "/Trial_" + i + "/bestIndividual.log"), true);
					os_best.write(("In generation: "+j+"\n").getBytes("UTF-8"));
					os_best.write(("Fitness: "+currentGen.individuals.get(0).fitness+"\n").getBytes("UTF-8"));
					os_best.write(("Chromosome: ").getBytes("UTF-8"));
					for(int l = 0; l < currentGen.individuals.get(0).chromosome.length; l++) {
						if(l > 0)
							os_best.write((",").getBytes("UTF-8"));
						os_best.write((Double.toString(currentGen.individuals.get(0).chromosome[l])).getBytes("UTF-8"));
					}
					os_best.write(("\n").getBytes("UTF-8"));
					os_best.write(("Strategy: ").getBytes("UTF-8"));
					for(int l = 0; l < currentGen.individuals.get(0).strategy.length; l++) {
						if(l > 0)
							os_best.write((",").getBytes("UTF-8"));
						os_best.write((Double.toString(currentGen.individuals.get(0).strategy[l])).getBytes("UTF-8"));
					}
					os_best.write(("\n").getBytes("UTF-8"));
					os_best.close();
				}
				//-----------------------------------------------------------------------------//

				// Swap currentGen and nextGen
				Population tmp = currentGen;
				currentGen = nextGen;
				nextGen = tmp;
			}
			
			//------------Log--------------------------------------------------------------//
			os_fitness.close();
			os_overview.close();
			//-----------------------------------------------------------------------------//

		}

		//End of Program, problem occurred in Spark 2.x.x
	    	//System.exit(0);
	}

	/// ES operators
	private static class ESOperator {
		// Initialize individuals randomly and reset fitness to 0
		public static void Initialize(Population pop) {
			for(Population.Individual i : pop.individuals) {
				int index = 0;
				for(int j = 0; j < pop.dimensions.length; j++) {
					for(int k = 0; k < pop.dimensions[j]; k++, index++) {
						i.chromosome[index] = randgen.nextDouble() * (pop.ucv[j] - pop.lcv[j]) + pop.lcv[j];
						i.strategy[index] = pop.initialsv[j];
					}
				}
				i.fitness = 0.0;
			}
		}

		// Evaluate individuals' fitness
		public static void Evaluate(Population pop, JavaSparkContext sc, Evaluator evaluator) {
			JavaRDD<Population.Individual> input = sc.parallelize(pop.individuals);
			JavaRDD<Double> output = input.map(evaluator);
			List<Double> fitness = output.collect();
			for(int i = 0; i < pop.individuals.size(); i++)
				pop.individuals.get(i).fitness = fitness.get(i);
		}

		// Sort the population and return the best mu individuals
		public static List<Population.Individual> SelectParent(Population pop, int mu) {
			Collections.sort(pop.individuals, new Comparator<Population.Individual>() {
				public int compare(Population.Individual individual_1, Population.Individual individual_2) {
					if(individual_1.fitness > individual_2.fitness) {
						return -1;
					} else if(individual_1.fitness < individual_2.fitness) {
						return 1;
					} else {
						return 0;
					}
				}			
			});
			return pop.individuals.subList(0, mu);
		}

		// Generate the new population using recombination
		public static void Recombine(Population pop, List<Population.Individual> parents) {
			for(Population.Individual i : pop.individuals) {
				int p1 = randgen.nextInt(parents.size());
				int p2 = randgen.nextInt(parents.size());
				for(int j = 0; j < i.chromosome.length; j++) {
					if(randgen.nextBoolean()) {
						i.chromosome[j] = parents.get(p1).chromosome[j];
						i.strategy[j] = parents.get(p1).strategy[j];
					} else {
						i.chromosome[j] = parents.get(p2).chromosome[j];
						i.strategy[j] = parents.get(p2).strategy[j];
					}
				}
			}
		}

		// Mutate each individual in the population
		public static void Mutate(Population pop) {
			for(Population.Individual i : pop.individuals) {
				int index = 0;
				for(int j = 0; j < pop.dimensions.length; j++) {
					double n1 = randgen.nextGaussian();
					for(int k = 0; k < pop.dimensions[j]; k++, index++) {
						double n2 = randgen.nextGaussian();
						double n3 = randgen.nextGaussian();
						i.strategy[index] *= Math.exp(pop.tauDash[j]*n1 + pop.tau[j]*n2);
						i.strategy[index] = Math.max(pop.lsv[j], i.strategy[index]);
						i.strategy[index] = Math.min(pop.usv[j], i.strategy[index]);
						i.chromosome[index] += i.strategy[index]*n3;
						i.chromosome[index] = Math.max(pop.lcv[j], i.chromosome[index]);
						i.chromosome[index] = Math.min(pop.ucv[j], i.chromosome[index]);
					}
				}
			}			
	
		}

		private static final Random randgen = new Random(System.currentTimeMillis());
	}

	/// ES population
	private static class Population {
		Population(int NP, int[] dimensions, double[] ucv, double[] lcv, double[] usv, double[] lsv, double[] initialsv) {
			individuals = new ArrayList<Individual>();
			int d_total = 0;
			for(int i : dimensions)
				d_total += i;
			for(int i = 0; i < NP; i++)
				individuals.add(new Individual(d_total));
			this.dimensions = dimensions.clone();
			this.ucv = ucv.clone();
			this.lcv = lcv.clone();
			this.usv = usv.clone();
			this.lsv = lsv.clone();
			this.initialsv = initialsv.clone();
			tau = new double[this.dimensions.length];
			tauDash = new double[this.dimensions.length];
			for(int i = 0; i < this.dimensions.length; i++) {
				tau[i] = 1.0 / Math.sqrt( 2.0 * Math.sqrt( this.dimensions[i] ) );
				tauDash[i] = 1.0 / Math.sqrt( 2.0 * this.dimensions[i] ); 
			}
		}
			
		// Individuals
		public List<Individual> individuals;
				
		// Individual description
		public int[] dimensions;			//dimensions of individual
	
		public double[] ucv;				//upper_bounds of chromosomes
		public double[] lcv;				//lower_bounds of chromosomes
		public double[] usv;				//upper_bounds of strategies
		public double[] lsv;				//lower_bounds of strategies
		public double[] initialsv;			//initial value for strategies
		
		public double[] tau;				//parameters for ES mutation
		public double[] tauDash;			//parameters for ES mutation
	
		/// ES individual
		public static class Individual implements Serializable {
			Individual(int dimension) {
				chromosome = new double[dimension];
				strategy = new double[dimension];
				fitness = 0.0;
			}

			public double[] chromosome;
			public double[] strategy;
			public double fitness;
		} 
	}


	/// Evaluator Class 
	private static class Evaluator implements Serializable, Function<Population.Individual, Double> {
		// Constructor with job settings
		Evaluator(String libName, int funcIndex) {
			this.libName = libName;
			this.funcIndex = funcIndex;
		}

		// Used for map transformation
		public Double call(Population.Individual x) {
			NativeLibLoader.setNativeLib(libName);
			return NativeLibLoader.JOBNATIVELIB.INSTANCE.evaluateFcns(x.chromosome, funcIndex);
		}

		/// Native lib Loader
		private static class NativeLibLoader {
			// Set native library
			public static void setNativeLib(String libName) {
				NativeLibLoader.libName = libName;
			}

			// JNA interface
			public interface JOBNATIVELIB extends Library {
				JOBNATIVELIB INSTANCE = (JOBNATIVELIB)Native.loadLibrary(libName, JOBNATIVELIB.class);
				double evaluateFcns(double individual[], int func_index);
			}

			private static String libName;
		}
	
		private final String libName;
		private final int funcIndex;
	}


	/// Job Settings Class
	private static class ESJobSettings {
		// Default constructor
		ESJobSettings() {
			outputPath = "StandardESv" + VERSION_MAJOR + "." + VERSION_MINOR + "_output_" + System.currentTimeMillis();
			nativeLib = "";
			funcIndex = 0;
			trialsNum = 10;
			
			dimensions = new int[1];
			maxGeneration = 500;
			
			lambda = 200;
			mu = 32;
			
			ucv	= new double[1];			
			lcv	= new double[1];			
			usv	= new double[1];
			lsv	= new double[1];
			initialsv = new double[1];			
			
			dimensions[0] = 300;
			ucv[0] = 1.0;
			lcv[0] = -1.0;
			usv[0] = 0.15;
			lsv[0] = 0.00001;
			initialsv[0] = 0.05;	
		}

		// Constructor with args
		ESJobSettings(String[] args) throws Exception {
			// Call default constructor
			this();
			
			// Specified args in command line
			try {		
				for(int i = 0; i < args.length; i++) {
					switch(args[i]) {
						case "-G":
							setMaxGeneration(Integer.parseInt(args[++i]));
							break;
						case "-OUTPUTPATH":
							setOutputPath(args[++i]);
							break;		
						case "-LAMBDA":
							setLambda(Integer.parseInt(args[++i]));
							break;
						case "-MU":
							setMu(Integer.parseInt(args[++i]));
							break;
						case "-LIB":
							setNativeLib(args[++i]);
							break;
						case "-FUNC_INDEX":
							setFuncIndex(Integer.parseInt(args[++i]));
							break;
						case "-TRIALS":
							setTrialsNum(Integer.parseInt(args[++i]));
							break;
						case "-D_FORMAT":
							int D_COUNT = Integer.parseInt(args[++i]);
							dimensions = new int[D_COUNT];
							ucv = new double[D_COUNT];
							lcv = new double[D_COUNT];
							usv = new double[D_COUNT];
							lsv = new double[D_COUNT];
							initialsv = new double[D_COUNT];
							for(int j = 0; j < D_COUNT; j++) {
								String[] D_CONF = args[++i].split(",");
								if(D_CONF.length != 6) {
									System.err.println("Bad format for option 'D_FORMAT' ");
									throw new Exception();
								}
								dimensions[j] = Integer.parseInt(D_CONF[0]);
								ucv[j] = Double.parseDouble(D_CONF[1]);
								lcv[j] = Double.parseDouble(D_CONF[2]);
								usv[j] = Double.parseDouble(D_CONF[3]);
								lsv[j] = Double.parseDouble(D_CONF[4]);
								initialsv[j] = Double.parseDouble(D_CONF[5]);
							}
							break;
						default:
							System.err.println("Undefined option: '" + args[i] + "'");
							throw new Exception(); 
					}
				}		
			} catch(Exception e) {
				System.err.println("Unknown command");
				System.err.println("Usage: spark-submit --num-executors NUM --files LIBNAME --name APPNAME StandardESv" + VERSION_MAJOR + "." + VERSION_MINOR + ".jar [<option>..]");
				System.err.println("Options (* must be specified):");
				System.err.println("-OUTPUTPATH <PATH> 			: Path for Outputs.");				
				System.err.println("-TRIALS <NUM> 			: Number of trials");
				System.err.println("-G <NUM> 				: Generation.");
				System.err.println("-LAMBDA <NUM> 			: Number of offspring.");
				System.err.println("-MU <NUM> 				: Number of parents.");
				System.err.println("-D_FORMAT <@SEE SAMPLES> 		: Describe the individual.");
				System.err.println("-LIB <NAME> (*) 			: Name of native library.");
				System.err.println("-FUNC_INDEX <NUM> 			: Index of function in native library to be used.");
				System.exit(-1);
			}
		}
		
		// Spark Task Parameters
		private String outputPath;			//path for outputs
		private String nativeLib;			//native library used for evaluation on "hdfs://"
		private int funcIndex;				//index of function in native library to be used
		private int trialsNum;				//number of trials
				
		// ES Parameters
		private int dimensions[];			//dimensions of individual	
		private int maxGeneration;			//max generation
		
		private int lambda;				//number of offspring 
		private int mu;					//number of parents

		private double[] ucv;				//upper_bounds of chromosomes
		private double[] lcv;				//lower_bounds of chromosomes
		private double[] usv;				//upper_bounds of strategies
		private double[] lsv;				//lower_bounds of strategies
		private double[] initialsv;			//initial value for strategies
		
		// Getters
		public String getOutputPath() {
			return outputPath;
		}
		public String getNativeLib() {
			return nativeLib;
		}
		public int getFuncIndex() {
			return funcIndex;
		}
		public int getTrialsNum() {
			return trialsNum;
		}
		public int[] getDimensions() {
			return dimensions;
		}
		public int getMaxGeneration() {
			return maxGeneration;
		}
		public int getLambda() {
			return lambda;
		}
		public int getMu() {
			return mu;
		}
		public double[] getUcv() {
			return ucv;
		}
		public double[] getLcv() {
			return lcv;
		}
		public double[] getUsv() {
			return usv;
		}
		public double[] getLsv() {
			return lsv;
		}
		public double[] getInitialsv() {
			return initialsv;
		}
		
		// Setters
		public void setOutputPath(final String value) {
			outputPath = value;
		}
		public void setNativeLib(final String value) {
			nativeLib = value;
		}
		public void setFuncIndex(final int value) {
			funcIndex = value;
		}
		public void setTrialsNum(final int value) {
			trialsNum = value;
		}
		public void setDimensions(final int[] value) {
			dimensions = value;
		}
		public void setMaxGeneration(final int value) {
			maxGeneration = value;
		}
		public void setLambda(final int value) {
			lambda = value;
		}
		public void setMu(final int value) {
			mu = value;
		}
		public void setUcv(final double[] value) {
			ucv = value;
		}
		public void setLcv(final double[] value) {
			lcv = value;
		}
		public void setUsv(final double[] value) {
			usv = value;
		}
		public void setLsv(final double[] value) {
			lsv = value;
		}
		public void setInitialsv(final double[] value) {
			initialsv = value;
		}
	}
}
