package com.example

import com.example.sr.CloudProps
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

class SpecBase
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport {

  val config: CloudProps = CloudProps.create()

}
