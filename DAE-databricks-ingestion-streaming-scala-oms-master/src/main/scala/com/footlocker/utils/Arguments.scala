
package com.footlocker.utils

object Arguments {

  def argsToMap(args: Array[String]): Map[String, String] = {

    def nextArgument(argList: List[String], map: Map[String, String]): Map[String, String] = {
      val pattern = "--(\\w+)".r // Selects Arg from --Arg
      val patternSwitch = "-(\\w+)".r // Selects Arg from -Arg

      argList match {
        case Nil => map
        case pattern(opt) :: value :: tail if !value.startsWith("-") => nextArgument(tail, map ++ Map(opt -> value))
        case patternSwitch(opt) :: tail => nextArgument(tail, map ++ Map(opt -> null))
        case string :: Nil => map ++ Map(string -> null)
        case option :: tail =>
          println(s"Unknown or malformed option while parsing args: '${option}'")
          sys.exit(1)
      }
    }

    nextArgument(args.toList, Map())
  }

}
