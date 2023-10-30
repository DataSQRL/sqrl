package com.datasqrl.data;

import com.datasqrl.cmd.AbstractGenerateCommand;
import com.datasqrl.util.Configuration;
import com.datasqrl.util.SerializerUtil;
import com.datasqrl.util.WriterUtil;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

@CommandLine.Command(name = "sensors", description = "Generates IoT sensor data")
public class GenerateSensors extends AbstractGenerateCommand {

  public static final String SENSOR_FILE = "sensors_part%04d.json";

  public static final String READINGS_FILE = "sensorreading_part%04d.csv";

  public static final String GROUP_FILE = "observationgroup_part%04d.json";

  public static final String GROUP_NAME = " Group";


  public static final int SENSOR_READINGS_PER_DAY = 3600*24; //one per second
  public static final ChronoUnit SENSOR_READING_UNIT = ChronoUnit.SECONDS;

  @Override
  public void run() {
    initialize();
    Config config = getConfiguration(new Config());

    long numDays = Math.max(1,root.getNumber()/(config.numSensors*SENSOR_READINGS_PER_DAY));
    Instant startTime = getStartTime(numDays);

    int numMachines = config.numSensors / config.avgSensorsPerMachine;
    List<Patient> machines = IntStream.range(0,numMachines).mapToObj(i -> new Patient(i)).collect(
        Collectors.toList());

    Instant initialSensorPlacement = startTime.minus(1, ChronoUnit.DAYS);
    List<Sensor> sensors = IntStream.range(0,config.numSensors)
        .mapToObj(i -> new Sensor(i, sampler.next(machines).patientId ,initialSensorPlacement,
            config.useEpoch))
        .collect(Collectors.toList());

    WriterUtil.writeToFile(sensors, getOutputDir().resolve(String.format(SENSOR_FILE,0)), null, null);

    Map<Sensor,SpikeGenerator> sensorMap = new HashMap<>(sensors.size());
    sensors.forEach(s -> sensorMap.put(s, new SpikeGenerator(sampler,
        sampler.nextDouble(config.temperatureBaselineMin, config.temperatureBaselineMax),
        config.maxNoise,
        sampler.nextDouble(1.0,config.maxTemperaturePeakStdDev),
        sampler.nextInt(config.minMaxRampWidth, config.maxMaxRampWidth),
        config.errorProbability
        )));

    long totalRecords = 0;
    int machineGroupId = 1;
    Instant startOfDay = startTime;
    for (int i = 0; i < numDays; i++) {
      int numReadings = (int)Math.min(SENSOR_READINGS_PER_DAY, (root.getNumber()-totalRecords)/config.numSensors);
      List<SensorReading> readings = new ArrayList<>(numReadings*config.numSensors);
      for (int j = 0; j < numReadings; j++) {
        Instant timestamp = startOfDay.plus(j,SENSOR_READING_UNIT);
        for (Map.Entry<Sensor, SpikeGenerator> sensorEntry : sensorMap.entrySet()) {
          Sensor sensor = sensorEntry.getKey();
          readings.add(new SensorReading(sensor.id, timestamp,
              sensorEntry.getValue().nextValue(),
              sampler.nextInt(0,100), config.useEpoch));
        }
      }
      WriterUtil.writeToFileSorted(readings, getOutputDir().resolve(String.format(READINGS_FILE,i+1)),
          Comparator.comparing(SensorReading::getTime),
          SensorReading.header(), null);
      totalRecords += readings.size();

      int numReassignments = (int)Math.round(sampler.nextPositiveNormal(
          config.avgSensorReassignments, config.avgSensorReassignmentsDeviation));
      List<Sensor> reassignments = new ArrayList<>(numReassignments);
      for (int j = 0; j < numReassignments; j++) {
        Sensor sensor = sampler.next(sensors);
        reassignments.add(sensor.replaced(sampler.next(machines).patientId,
            sampler.nextTimestamp(startOfDay, 1, ChronoUnit.DAYS)));
      }
      WriterUtil.writeToFileSorted(reassignments, getOutputDir().resolve(String.format(SENSOR_FILE,i+1)),
          Comparator.comparing(Sensor::getPlaced),
          null, null);

      //Machine Groups
      int numGroups = (int)Math.round(sampler.nextPositiveNormal(config.avgGroupPerDay,
          config.avgGroupPerDayDeviation));
      List<MachineGroup> groups = new ArrayList<>(numGroups);
      for (int j = 0; j < numGroups; j++) {
        int numMachinesInGroup = (int)Math.round(sampler.nextPositiveNormal(config.avgMachinesPerGroup,
            config.avgMachinesPerGroupDeviation));
        numMachinesInGroup = Math.min(machines.size(),numMachinesInGroup);
        groups.add(new MachineGroup(machineGroupId++,faker.medical().hospitalName()+GROUP_NAME,
            sampler.nextTimestamp(startOfDay, 1, ChronoUnit.DAYS).toString(),
            sampler.withoutReplacement(numMachinesInGroup,machines)));
        sampler.withoutReplacement(numMachinesInGroup,machines);
      }
      WriterUtil.writeToFileSorted(groups, getOutputDir().resolve(String.format(GROUP_FILE,i+1)),
          Comparator.comparing(MachineGroup::getCreated),
          null, null);

      startOfDay = startOfDay.plus(1, ChronoUnit.DAYS); //next day
    }
  }

  @Value
  @EqualsAndHashCode(onlyExplicitlyIncluded = true)
  public static class Sensor {

    @Include
    int id;
    int machineid;
    Instant placed;
    transient boolean useEpoch;

    @Override
    public String toString() {
      return SerializerUtil.toJson(Map.of("id",id,"patientid",machineid,
          "placed",useEpoch?placed.toEpochMilli():placed.toString()));
    }

    public Sensor replaced(int machineid, Instant placementTime) {
      return new Sensor(id, machineid, placementTime, useEpoch);
    }

  }

  @Value
  public static class SensorReading {

    int sensorid;
    Instant time;
    double temperature;
    double humidity;

    transient boolean useEpoch;

    @Override
    public String toString() {
      return Stream.of(sensorid, useEpoch?time.toEpochMilli():time.toString(), temperature/*, humidity*/).map(Objects::toString).collect(
          Collectors.joining(", "));
    }

    public static String header() {
      return StringUtils.join(new String[]{"sensorid", "time", "temperature"/*, "humidity"*/},", ");
    }

  }

  @Value
  public static class MachineGroup {

    int groupId;
    String groupName;
    String created;
    Collection<Patient> patients;

    @Override
    public String toString() {
      return SerializerUtil.toJson(this);
    }

  }

  @Value
  public static class Patient {

    int patientId;

  }

  public static class Config implements Configuration {

    public int numSensors = 30;

    public int avgSensorsPerMachine = 2;

    public int avgSensorReassignments = 3;

    public int avgSensorReassignmentsDeviation = 1;

    public int avgGroupPerDay = 5;

    public double avgGroupPerDayDeviation = 4.0;

    public int avgMachinesPerGroup = 10;

    public double avgMachinesPerGroupDeviation = 20.0;

    public double temperatureBaselineMin = 97;

    public double temperatureBaselineMax = 99;

    public double maxNoise = 0.05;

    public double maxTemperaturePeakStdDev = 2.0;

    public int minMaxRampWidth = 600;

    public int maxMaxRampWidth = 10000;

    public double errorProbability = 0.00;

    public boolean useEpoch = true;



    @Override
    public void scale(long scaleFactor, long number) {
      numSensors = (int)Math.min(10000000,Math.max(20, number/(86400*60)));
    }
  }


}
