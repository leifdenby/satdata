# Changelog

## [Unreleased](https://github.com/leifdenby/satdata/tree/HEAD)

[Full Changelog](https://github.com/leifdenby/satdata/compare/v0.2.3...) (2022-02-21)

*maintenance*

- switch to using `setup.cfg` rather than `setup.py` [\#20](https://github.com/leifdenby/satdata/pull/20) ([leifdenby](https://github.com/leifdenby))

- add test which runs CLI and add missing dependency on `isodate` package [\#19](https://github.com/leifdenby/satdata/pull/19) ([leifdenby](https://github.com/leifdenby))

## [v0.2.3](https://github.com/leifdenby/satdata/tree/v0.2.3)

[Full Changelog](https://github.com/leifdenby/satdata/compare/v0.2.2...v0.2.3) (2022-02-21)

*new features*

- add cloud-mask dataset [\#15](https://github.com/leifdenby/satdata/pull/15) ([leifdenby](https://github.com/leifdenby))
- add cloud particle size \(CPS\) field [\#14](https://github.com/leifdenby/satdata/pull/14) ([leifdenby](https://github.com/leifdenby))
- add cloud-top height product [\#9](https://github.com/leifdenby/satdata/pull/9) ([leifdenby](https://github.com/leifdenby))
- add CLI option for nearest-in-time file [\#11](https://github.com/leifdenby/satdata/pull/11) ([leifdenby](https://github.com/leifdenby))
- Add TOA shortwave reflection product [\#8](https://github.com/leifdenby/satdata/pull/8) ([leifdenby](https://github.com/leifdenby))

*bugfixes*

- fix mistake in zenith time calculation [\#13](https://github.com/leifdenby/satdata/pull/13) ([leifdenby](https://github.com/leifdenby))

*maintenance*

- add ci task for pypi deployment [\#16](https://github.com/leifdenby/satdata/pull/16) ([leifdenby](https://github.com/leifdenby))
- support products in cli [\#7](https://github.com/leifdenby/satdata/pull/7) ([leifdenby](https://github.com/leifdenby))
- support products in cli [\#6](https://github.com/leifdenby/satdata/pull/6) ([leifdenby](https://github.com/leifdenby))
- apply black and add to CI [\#5](https://github.com/leifdenby/satdata/pull/5) ([leifdenby](https://github.com/leifdenby))
- fixups for README [\#10](https://github.com/leifdenby/satdata/pull/10) ([leifdenby](https://github.com/leifdenby))
- switch to linting with pre-commit [\#18](https://github.com/leifdenby/satdata/pull/18)
- make radiance measurements (Rad) default when using cli [\#12](https://github.com/leifdenby/satdata/pull/12)


## [v0.2.2](https://github.com/leifdenby/satdata/tree/v0.2.2) (2020-11-28)

[Full Changelog](https://github.com/leifdenby/satdata/compare/v0.2.1...v0.2.2)

- Add TPW L2 product
- Fixes for query with numpy datetime
- Fixes for downloading L2 data

## [v0.2.1](https://github.com/leifdenby/satdata/tree/v0.2.1) (2020-04-22)

[Full Changelog](https://github.com/leifdenby/satdata/compare/v0.2.0...v0.2.1)

- Changed default output dir so that it maches with when we used boto3 directly

## [v0.2.0](https://github.com/leifdenby/satdata/tree/v0.2.0) (2020-04-09)

[Full Changelog](https://github.com/leifdenby/satdata/compare/v0.1.3...v0.2.0)

**Merged pull requests:**

- Add cli [\#4](https://github.com/leifdenby/satdata/pull/4) ([leifdenby](https://github.com/leifdenby))
- Move to s3fs instead of boto [\#3](https://github.com/leifdenby/satdata/pull/3) ([leifdenby](https://github.com/leifdenby))

## [v0.1.3](https://github.com/leifdenby/satdata/tree/v0.1.3) (2020-02-13)

[Full Changelog](https://github.com/leifdenby/satdata/compare/v0.1.2...v0.1.3)

## [v0.1.2](https://github.com/leifdenby/satdata/tree/v0.1.2) (2020-01-13)

[Full Changelog](https://github.com/leifdenby/satdata/compare/39481b5ea9fae41eb669a23b96ddaf60c0e51688...v0.1.2)

**Merged pull requests:**

- Multi hour queries [\#1](https://github.com/leifdenby/satdata/pull/1) ([leifdenby](https://github.com/leifdenby))
